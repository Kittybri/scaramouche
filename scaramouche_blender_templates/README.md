These are the current Scaramouche renderer scripts used for the local Blender presentation pipeline.

Included here:
- `notes_to_scaramouche_video.py`
- `notes_to_duo_debate_video.py`
- `render_scaramouche_plates.py`

Important:
- These scripts still depend on local Blender assets that are not stored in this repo, including the populated `.blend` files and model assets in your local template folders.
- The main fix in this snapshot is the real Scaramouche GLB import/render path, plus the solo compositor change that renders him as a proper presenter plate instead of using the broken full-stage frame.
