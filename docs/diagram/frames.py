"""Write N SVG frames of a D2 animated diagram: each dashed edge shifted by 1/N of its dash period, so the GIF loops seamlessly."""
import re
N = 12
src = open('kroki/pipeline.svg', encoding='utf-8').read()
pat = re.compile(r'stroke-dasharray:([\d.]+),([\d.]+);stroke-dashoffset:[-\d.]+;animation: dashdraw [^;"]+;?')
print('animated edges:', len(pat.findall(src)))
for k in range(N):
    out = pat.sub(lambda m: f'stroke-dasharray:{m[1]},{m[2]};stroke-dashoffset:{-(float(m[1]) + float(m[2])) * k / N:.3f};', src)
    open(f'kroki/f{k}.svg', 'w', encoding='utf-8').write(out)
