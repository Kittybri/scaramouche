"""macOS 13+ adapter. Only fixed OS operations; nothing from the wire becomes code."""

import asyncio
from .base import Platform, Sample

# Immutable scripts. Arguments are data, never interpolated AppleScript.
SCRIPTS = {
    "notify": 'on run argv\ndisplay notification (item 1 of argv) with title "Character Companion"\nend run',
    "volume": "on run argv\nset volume output volume ((item 1 of argv) as integer)\nend run",
    "lock": 'tell application "System Events" to keystroke "q" using {control down, command down}',
}


class MacOS(Platform):
    def __init__(self):
        self.sound = None

    def sample(self):
        import AppKit
        import Quartz as Q

        app = AppKit.NSWorkspace.sharedWorkspace().frontmostApplication()
        session = Q.CGSessionCopyCurrentDictionary()
        # A missing GUI session or a login/saver foreground is never considered safe.
        bundle = str(app.bundleIdentifier() or "") if app else ""
        locked = (
            not session
            or not session.get("kCGSessionOnConsoleKey", False)
            or bool(session.get("CGSSessionScreenIsLocked", False))
            or any(s in bundle.lower() for s in ("loginwindow", "screensaver"))
        )
        idle = Q.CGEventSourceSecondsSinceLastEventType(
            Q.kCGEventSourceStateCombinedSessionState, Q.kCGAnyInputEventType
        )
        window, title = 0, ""
        if app and not locked:
            for row in (
                Q.CGWindowListCopyWindowInfo(
                    Q.kCGWindowListOptionOnScreenOnly
                    | Q.kCGWindowListExcludeDesktopElements,
                    Q.kCGNullWindowID,
                )
                or []
            ):
                if (
                    row.get(Q.kCGWindowOwnerPID) == app.processIdentifier()
                    and row.get(Q.kCGWindowLayer) == 0
                ):
                    window, title = int(row[Q.kCGWindowNumber]), str(
                        row.get(Q.kCGWindowName, "")
                    )
                    break
        return Sample(bundle, float(idle), bool(locked), window, title)

    def capture(self, sample):
        import Quartz as Q
        import AppKit

        if not Q.CGPreflightScreenCaptureAccess():
            raise RuntimeError("screen_permission_required")
        image = Q.CGWindowListCreateImage(
            Q.CGRectNull,
            Q.kCGWindowListOptionIncludingWindow,
            sample.window,
            Q.kCGWindowImageBoundsIgnoreFraming,
        )
        if image is None:
            raise RuntimeError("capture_unavailable")
        bitmap = AppKit.NSBitmapImageRep.alloc().initWithCGImage_(image)
        return bytes(
            bitmap.representationUsingType_properties_(AppKit.NSPNGFileType, {})
        )

    async def action(self, action, value):
        if action in SCRIPTS:
            args = (
                [str(value)]
                if action == "notify"
                else (
                    [str(int(min(0.5, max(0, value)) * 100))]
                    if action == "volume"
                    else []
                )
            )
            # Fixed executable, script selected from constants, no shell, no code input.
            proc = await asyncio.create_subprocess_exec(
                "/usr/bin/osascript",
                "-e",
                SCRIPTS[action],
                *args,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                code = await asyncio.wait_for(proc.wait(), 5)
                if code:
                    raise RuntimeError("os_action_denied")
            finally:
                if proc.returncode is None:
                    proc.terminate()  # Only our bounded helper, never a user application.
                    await proc.wait()
        elif action in {"open_url", "launch_app"}:
            import AppKit
            from Foundation import NSURL

            url = (
                NSURL.URLWithString_(value)
                if action == "open_url"
                else NSURL.fileURLWithPath_(value)
            )
            if not AppKit.NSWorkspace.sharedWorkspace().openURL_(url):
                raise RuntimeError("launch_failed")
        elif action == "play":
            import AppKit
            from Foundation import NSData

            await self.stop()
            data, volume = value
            sound = AppKit.NSSound.alloc().initWithData_(
                NSData.dataWithBytes_length_(data, len(data))
            )
            if not sound:
                raise RuntimeError("unsupported_audio")
            self.sound = sound
            sound.setVolume_(volume)
            try:
                if not sound.play():
                    raise RuntimeError("audio_unavailable")
                for _ in range(48):
                    if not sound.isPlaying():
                        break
                    await asyncio.sleep(0.25)
            finally:
                sound.stop()
                if self.sound is sound:
                    self.sound = None
        else:
            raise RuntimeError("unsupported_action")

    async def stop(self):
        if self.sound:
            self.sound.stop()
            self.sound = None
