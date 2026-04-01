import json
import math
import os
import sys

import bpy
from mathutils import Matrix, Vector


SCARA_MODEL_CANDIDATES = [
    os.getenv("SCARAMOUCHE_MODEL_PATH", "").strip(),
    r"C:\Users\abria\Downloads\genshin-impact-scaramouche (1).glb",
    r"C:\Users\abria\Downloads\genshin-impact-scaramouche.glb",
]
GUIDE_HEAD_STEMS = ("WANDERER_GUIDE_HEAD", "PARTNER_GUIDE_HEAD")
GUIDE_TORSO_STEMS = ("WANDERER_GUIDE_TORSO", "PARTNER_GUIDE_TORSO")
RIGHT_SHOULDER_BONE = "Bone e58fb3e882a9_019"
LEFT_SHOULDER_BONE = "Bone e5b7a6e882a9_037"
RIGHT_UPPER_ARM_BONE = "Bone e58fb3e88595_021"
LEFT_UPPER_ARM_BONE = "Bone e5b7a6e88595_039"
RIGHT_FOREARM_BONE = "Bone e58fb3e381b2e38198_026"
LEFT_FOREARM_BONE = "Bone e5b7a6e381b2e38198_044"
RIGHT_WRIST_BONE = "Bone e58fb3e6898be9a696_033"
LEFT_WRIST_BONE = "Bone e5b7a6e6898be9a696_049"


def read_arg(name: str, default: str = "") -> str:
    argv = sys.argv
    if "--" not in argv:
        return default
    args = argv[argv.index("--") + 1 :]
    if name in args:
        idx = args.index(name)
        if idx + 1 < len(args):
            return args[idx + 1]
    return default


def find_stem(scene, stem: str):
    return next((obj for obj in scene.objects if obj.name.split(".")[0] == stem), None)


def find_screen(scene):
    return find_stem(scene, "SLIDE_SCREEN")


def collect_descendants(root):
    descendants = []
    stack = list(root.children)
    seen = set()
    while stack:
        obj = stack.pop()
        if obj in seen:
            continue
        seen.add(obj)
        descendants.append(obj)
        stack.extend(list(obj.children))
    return descendants


def find_scara_root(scene):
    for obj in scene.objects:
        if obj.name.startswith("Sketchfab_model"):
            return obj
    return None


def find_scara_container(root):
    if root is None:
        return None
    return next((obj for obj in root.children if obj.name.startswith("Scaramouche.fbx")), None)


def find_scara_armature(scene, root):
    if root is None:
        return None
    descendants = collect_descendants(root)
    return next((obj for obj in descendants if obj.type == "ARMATURE"), None)


def find_scara_meshes(scene, root):
    if root is None:
        return []
    descendants = collect_descendants(root)
    return [obj for obj in descendants if obj.type == "MESH" and obj.name != "Icosphere"]


def find_face_mesh(meshes):
    for mesh in meshes:
        data_name = getattr(mesh.data, "name", "")
        if data_name.startswith("Scaramouche_Face"):
            return mesh
    return next((mesh for mesh in meshes if "Face" in getattr(mesh.data, "name", "")), None)


def bbox_world(objects):
    minimum = Vector((1e9, 1e9, 1e9))
    maximum = Vector((-1e9, -1e9, -1e9))
    found = False
    for obj in objects:
        if obj.type != "MESH":
            continue
        found = True
        for corner in obj.bound_box:
            world = obj.matrix_world @ Vector(corner)
            minimum.x = min(minimum.x, world.x)
            minimum.y = min(minimum.y, world.y)
            minimum.z = min(minimum.z, world.z)
            maximum.x = max(maximum.x, world.x)
            maximum.y = max(maximum.y, world.y)
            maximum.z = max(maximum.z, world.z)
    if not found:
        return Vector((0.0, 0.0, 0.0)), Vector((0.0, 0.0, 0.0))
    return minimum, maximum


def resolve_scara_model_path():
    for candidate in SCARA_MODEL_CANDIDATES:
        if candidate and os.path.exists(candidate):
            return candidate
    return ""


def set_active_scene(scene):
    window = getattr(bpy.context, "window", None)
    if window is not None:
        window.scene = scene


def purge_existing_scara(scene):
    roots = [obj for obj in scene.objects if obj.name.startswith("Sketchfab_model")]
    doomed = set()
    for root in roots:
        doomed.add(root)
        doomed.update(collect_descendants(root))
    doomed.update({obj for obj in scene.objects if obj.name == "Icosphere"})
    for obj in doomed:
        try:
            bpy.data.objects.remove(obj, do_unlink=True)
        except Exception:
            pass


def import_scara_model(scene):
    model_path = resolve_scara_model_path()
    if not model_path:
        return find_scara_root(scene)
    set_active_scene(scene)
    before = set(bpy.data.objects)
    bpy.ops.import_scene.gltf(filepath=model_path)
    imported = [obj for obj in bpy.data.objects if obj not in before]
    imported_names = [obj.name for obj in imported]
    for name in imported_names:
        obj = bpy.data.objects.get(name)
        if obj is None:
            continue
        if obj.name == "Icosphere":
            try:
                bpy.data.objects.remove(obj, do_unlink=True)
            except Exception:
                pass
    imported_roots = []
    for name in imported_names:
        obj = bpy.data.objects.get(name)
        if obj is None:
            continue
        if obj.parent is None and obj.name.startswith("Sketchfab_model"):
            imported_roots.append(obj)
    if imported_roots:
        return imported_roots[0]
    return find_scara_root(scene)


def apply_screen_image(scene, image_path: str):
    screen = find_screen(scene)
    if screen is None or not image_path:
        return
    material = screen.active_material
    if material is None:
        return
    material.use_nodes = True
    nodes = material.node_tree.nodes
    links = material.node_tree.links
    principled = nodes.get("Principled BSDF")
    image_node = nodes.get("SlideImage")
    if image_node is None:
        image_node = nodes.new("ShaderNodeTexImage")
        image_node.name = "SlideImage"
        image_node.location = (-420, 220)
    image_node.image = bpy.data.images.load(image_path, check_existing=True)
    for socket_name in ("Base Color", "Emission Color"):
        socket = principled.inputs[socket_name]
        for link in list(socket.links):
            links.remove(link)
        links.new(image_node.outputs["Color"], socket)
    principled.inputs["Emission Strength"].default_value = 1.2


def stabilize_materials(meshes):
    for obj in meshes:
        for mat in obj.data.materials:
            if not mat or not mat.use_nodes:
                continue
            if getattr(mat, "blend_method", "") == "BLEND":
                mat.blend_method = "HASHED"
            if getattr(mat, "shadow_method", "") == "NONE":
                mat.shadow_method = "HASHED"
            try:
                mat.use_backface_culling = True
            except Exception:
                pass
            principled = mat.node_tree.nodes.get("Principled BSDF")
            if principled:
                principled.inputs["Roughness"].default_value = 0.72


def reset_pose(armature):
    if armature is None:
        return
    for bone in armature.pose.bones:
        bone.rotation_mode = "XYZ"
        bone.rotation_euler = (0.0, 0.0, 0.0)
        bone.location = (0.0, 0.0, 0.0)
        bone.scale = (1.0, 1.0, 1.0)


def apply_presenter_pose(armature):
    if armature is None:
        return
    reset_pose(armature)
    pose_rotations = {
        RIGHT_SHOULDER_BONE: (0.0, 6.0, 4.0),
        LEFT_SHOULDER_BONE: (0.0, -6.0, -4.0),
        RIGHT_UPPER_ARM_BONE: (0.0, 0.0, 34.0),
        LEFT_UPPER_ARM_BONE: (0.0, 0.0, -34.0),
        RIGHT_FOREARM_BONE: (0.0, 0.0, -10.0),
        LEFT_FOREARM_BONE: (0.0, 0.0, 10.0),
        RIGHT_WRIST_BONE: (0.0, 0.0, 3.0),
        LEFT_WRIST_BONE: (0.0, 0.0, -3.0),
    }
    for bone_name, degrees_xyz in pose_rotations.items():
        pose_bone = armature.pose.bones.get(bone_name)
        if pose_bone is None:
            continue
        pose_bone.rotation_mode = "XYZ"
        pose_bone.rotation_euler = tuple(math.radians(value) for value in degrees_xyz)


def find_pose_bone(armature, name_fragment: str):
    if armature is None:
        return None
    lower = name_fragment.lower()
    for bone in armature.pose.bones:
        if lower in bone.name.lower():
            return bone
    return None


def apply_expression_pose(scene, mood: str):
    root = find_scara_root(scene)
    armature = find_scara_armature(scene, root)
    if armature is None:
        return
    apply_presenter_pose(armature)
    if mood == "debate":
        right = armature.pose.bones.get(RIGHT_SHOULDER_BONE)
        left = armature.pose.bones.get(LEFT_SHOULDER_BONE)
        if right is not None:
            right.rotation_euler.y += math.radians(4.0)
        if left is not None:
            left.rotation_euler.y -= math.radians(4.0)


def apply_mouth_pose(scene, viseme: str, mood: str):
    root = find_scara_root(scene)
    armature = find_scara_armature(scene, root)
    if armature is None:
        return
    apply_expression_pose(scene, mood)
    lower_tooth = find_pose_bone(armature, "ToothBone D")
    upper_tooth = find_pose_bone(armature, "ToothBone U")
    angle_map = {
        "rest": 0.0,
        "a": -10.0,
        "i": -4.0,
        "u": -6.0,
        "e": -7.0,
        "o": -12.0,
        "n": -2.0,
        "blink": 0.0,
    }
    angle = math.radians(angle_map.get(viseme, 0.0))
    if lower_tooth is not None:
        lower_tooth.rotation_mode = "XYZ"
        lower_tooth.rotation_euler.x = angle
    if upper_tooth is not None and viseme in {"a", "o"}:
        upper_tooth.rotation_mode = "XYZ"
        upper_tooth.rotation_euler.x = math.radians(1.5)
    bpy.context.view_layer.update()


def target_guides(scene):
    guide_head = None
    guide_torso = None
    for stem in GUIDE_HEAD_STEMS:
        guide_head = find_stem(scene, stem)
        if guide_head is not None:
            break
    for stem in GUIDE_TORSO_STEMS:
        guide_torso = find_stem(scene, stem)
        if guide_torso is not None:
            break
    return guide_head, guide_torso


def normalize_scara(scene, presenter_only: bool):
    purge_existing_scara(scene)
    root = import_scara_model(scene)
    if root is None:
        raise SystemExit("Could not import Scaramouche GLB for rendering.")

    root.parent = None
    root.matrix_parent_inverse = Matrix.Identity(4)
    root.location = (0.0, 0.0, 0.0)
    root.rotation_euler = (0.0, 0.0, 0.0)
    root.scale = (1.0, 1.0, 1.0)

    container = find_scara_container(root)
    if container is not None:
        container.rotation_mode = "XYZ"
        container.rotation_euler = (math.radians(-90.0), 0.0, math.radians(180.0))

    armature = find_scara_armature(scene, root)
    apply_presenter_pose(armature)
    bpy.context.view_layer.update()

    meshes = find_scara_meshes(scene, root)
    face_mesh = find_face_mesh(meshes)
    if not meshes or face_mesh is None:
        raise SystemExit("Could not find Scaramouche meshes after import.")

    model_min, model_max = bbox_world(meshes)
    face_min, face_max = bbox_world([face_mesh])
    face_center = (face_min + face_max) / 2.0

    guide_head, guide_torso = target_guides(scene)
    target_x = 0.0 if presenter_only else -1.2
    target_y = 0.0 if presenter_only else 0.25
    target_face_z = 1.18 if presenter_only else 0.98
    if guide_head is not None and not presenter_only:
        target_x = guide_head.matrix_world.translation.x
        target_y = guide_head.matrix_world.translation.y
    face_offset_from_floor = max(0.001, face_center.z - model_min.z)
    scale = target_face_z / face_offset_from_floor
    root.scale = (scale, scale, scale)

    bpy.context.view_layer.update()
    model_min, model_max = bbox_world(meshes)
    face_min, face_max = bbox_world([face_mesh])
    face_center = (face_min + face_max) / 2.0
    root.location += Vector((target_x - face_center.x, target_y - face_center.y, -model_min.z))
    bpy.context.view_layer.update()

    model_min, model_max = bbox_world(meshes)
    face_min, face_max = bbox_world([face_mesh])
    face_center = (face_min + face_max) / 2.0
    stabilize_materials(meshes)

    return {
        "root": root,
        "meshes": meshes,
        "armature": armature,
        "face_center": face_center,
        "model_min": model_min,
        "model_max": model_max,
        "guide_head": guide_head,
        "guide_torso": guide_torso,
    }


def point_camera(camera, target: Vector):
    direction = target - camera.location
    camera.rotation_euler = direction.to_track_quat("-Z", "Y").to_euler()


def configure_camera(scene, presenter_only: bool, layout: dict):
    camera = scene.camera
    if camera is None:
        camera_data = bpy.data.cameras.new("ScaramoucheRenderCamera")
        camera = bpy.data.objects.new("ScaramoucheRenderCamera", camera_data)
        scene.collection.objects.link(camera)
        scene.camera = camera
    camera.data.sensor_width = 36.0

    face_center = layout["face_center"]
    model_min = layout["model_min"]
    model_max = layout["model_max"]
    guide_torso = layout["guide_torso"]
    screen = find_screen(scene)

    if presenter_only:
        focus = Vector((face_center.x, face_center.y + 0.02, model_min.z + (model_max.z - model_min.z) * 0.62))
        camera.location = Vector((face_center.x, face_center.y - 1.95, model_min.z + (model_max.z - model_min.z) * 0.70))
        camera.data.lens = 58.0
    else:
        torso_target = guide_torso.matrix_world.translation if guide_torso is not None else Vector((face_center.x, face_center.y, model_min.z + 0.58 * (model_max.z - model_min.z)))
        screen_center = screen.matrix_world.translation if screen is not None else Vector((1.94, 0.44, 1.69))
        focus = Vector(
            (
                (torso_target.x + screen_center.x) * 0.5,
                max(torso_target.y, screen_center.y) + 0.16,
                (torso_target.z + screen_center.z) * 0.5,
            )
        )
        camera.location = Vector((focus.x + 0.12, focus.y - 6.35, focus.z + 0.22))
        camera.data.lens = 41.0

    point_camera(camera, focus)
    return camera


def configure_scene_visibility(scene, keep, presenter_only: bool):
    keep = set(keep)
    for obj in list(scene.objects):
        stem = obj.name.split(".")[0]
        if stem in {*GUIDE_HEAD_STEMS, *GUIDE_TORSO_STEMS}:
            obj.hide_render = True
            obj.hide_viewport = True
            continue
        if presenter_only and obj.type != "CAMERA" and obj not in keep:
            obj.hide_render = True
            obj.hide_viewport = True
            continue
        if obj in keep:
            obj.hide_render = False
            obj.hide_viewport = False


def main():
    blend_path = read_arg("--blend")
    scene_name = read_arg("--scene", "01_Lecture_Explainer")
    spec_path = read_arg("--spec")
    output_dir = read_arg("--output-dir")
    mood = read_arg("--mood", "debate")
    width = int(read_arg("--width", "1280"))
    height = int(read_arg("--height", "720"))
    engine = read_arg("--engine", "BLENDER_WORKBENCH")
    presenter_only = read_arg("--presenter-only", os.getenv("DUO_PRESENTER_ONLY", "")).strip().lower() in {"1", "true", "yes", "on"}

    if not blend_path or not os.path.exists(blend_path):
        raise SystemExit("Missing --blend <path>")
    if not spec_path or not os.path.exists(spec_path):
        raise SystemExit("Missing --spec <path>")
    if not output_dir:
        raise SystemExit("Missing --output-dir <path>")

    os.makedirs(output_dir, exist_ok=True)
    bpy.ops.wm.open_mainfile(filepath=blend_path)
    scene = bpy.data.scenes.get(scene_name)
    if scene is None:
        raise SystemExit(f"Scene not found: {scene_name}")
    set_active_scene(scene)

    with open(spec_path, "r", encoding="utf-8") as handle:
        spec = json.load(handle)

    layout = normalize_scara(scene, presenter_only=presenter_only)
    configure_camera(scene, presenter_only=presenter_only, layout=layout)
    keep = {layout["root"], layout["armature"], *layout["meshes"]}
    configure_scene_visibility(scene, keep, presenter_only=presenter_only)

    scene.render.resolution_x = width
    scene.render.resolution_y = height
    scene.render.resolution_percentage = 100
    scene.render.image_settings.file_format = "PNG"
    if presenter_only:
        scene.render.image_settings.color_mode = "RGBA"
        try:
            scene.render.film_transparent = True
        except Exception:
            pass
    try:
        scene.render.engine = engine
    except Exception:
        pass
    if scene.render.engine == "BLENDER_WORKBENCH":
        try:
            scene.display.shading.color_type = "TEXTURE"
            scene.display.shading.light = "STUDIO"
            scene.display.shading.show_object_outline = False
        except Exception:
            pass
    scene.frame_current = max(1, scene.frame_end // 2)

    visemes = ["rest", "a", "i", "u", "e", "o", "n", "blink"]
    output_manifest = {"plates": {}}

    for index, segment in enumerate(spec.get("segments", []), start=1):
        slide_path = segment.get("slide_path", "")
        apply_screen_image(scene, slide_path)
        output_manifest["plates"][str(index)] = {}
        for viseme in visemes:
            apply_mouth_pose(scene, viseme, mood)
            target = os.path.join(output_dir, f"segment_{index:02d}_{viseme}.png")
            scene.render.filepath = target
            bpy.ops.render.render(write_still=True, scene=scene.name)
            output_manifest["plates"][str(index)][viseme] = target

    with open(os.path.join(output_dir, "plates_manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(output_manifest, handle, indent=2)
    print(json.dumps(output_manifest, indent=2))


if __name__ == "__main__":
    main()
