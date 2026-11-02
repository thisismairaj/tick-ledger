from PIL import Image
frames = [Image.open(f"f{k}.png").convert("RGB") for k in range(12)]
pal = frames[0].convert("P", palette=Image.ADAPTIVE, colors=128)
small = [f.quantize(palette=pal, dither=0) for f in frames]
small[0].save("cr-lead.gif", save_all=True, append_images=small[1:], duration=80, loop=0, optimize=True)
