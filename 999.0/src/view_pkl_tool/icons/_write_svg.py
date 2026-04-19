import pathlib

svg = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" width="64" height="64">\n'
    '  <defs>\n'
    '    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">\n'
    '      <stop offset="0%" stop-color="#0f2a3f"/>\n'
    '      <stop offset="100%" stop-color="#0a1a2e"/>\n'
    '    </linearGradient>\n'
    '    <linearGradient id="lidGrad" x1="0" y1="0" x2="0" y2="1">\n'
    '      <stop offset="0%" stop-color="#e8c84a"/>\n'
    '      <stop offset="100%" stop-color="#b8922a"/>\n'
    '    </linearGradient>\n'
    '    <linearGradient id="jarGrad" x1="0" y1="0" x2="0" y2="1">\n'
    '      <stop offset="0%" stop-color="#d4f0e8"/>\n'
    '      <stop offset="100%" stop-color="#9fd8c4"/>\n'
    '    </linearGradient>\n'
    '    <filter id="glow" x="-20%" y="-20%" width="140%" height="140%">\n'
    '      <feGaussianBlur stdDeviation="1.2" result="blur"/>\n'
    '      <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>\n'
    '    </filter>\n'
    '  </defs>\n'
    '  <rect width="64" height="64" rx="12" ry="12" fill="url(#bg)"/>\n'
    '  <rect x="18" y="22" width="28" height="30" rx="5" ry="5" fill="url(#jarGrad)" opacity="0.92"/>\n'
    '  <rect x="21" y="25" width="5" height="18" rx="2.5" fill="white" opacity="0.25"/>\n'
    '  <rect x="16" y="17" width="32" height="8" rx="3" ry="3" fill="url(#lidGrad)"/>\n'
    '  <rect x="20" y="19" width="24" height="4" rx="2" fill="#f5d96a" opacity="0.5"/>\n'
    '  <g filter="url(#glow)" opacity="0.85">\n'
    '    <rect x="23" y="30" width="4" height="2.5" rx="1" fill="#1a6b4a"/>\n'
    '    <rect x="29" y="30" width="2.5" height="2.5" rx="1" fill="#1a6b4a"/>\n'
    '    <rect x="33" y="30" width="4" height="2.5" rx="1" fill="#1a6b4a"/>\n'
    '    <rect x="23" y="34.5" width="2.5" height="2.5" rx="1" fill="#1a6b4a"/>\n'
    '    <rect x="27.5" y="34.5" width="4" height="2.5" rx="1" fill="#1a6b4a"/>\n'
    '    <rect x="33.5" y="34.5" width="3" height="2.5" rx="1" fill="#1a6b4a"/>\n'
    '    <rect x="23" y="39" width="4" height="2.5" rx="1" fill="#1a6b4a"/>\n'
    '    <rect x="29" y="39" width="2.5" height="2.5" rx="1" fill="#1a6b4a"/>\n'
    '    <rect x="33" y="39" width="3.5" height="2.5" rx="1" fill="#1a6b4a"/>\n'
    '  </g>\n'
    '  <circle cx="46" cy="18" r="7" fill="none" stroke="#4ecdc4" stroke-width="2.2" opacity="0.95"/>\n'
    '  <circle cx="46" cy="18" r="4.5" fill="#4ecdc4" opacity="0.18"/>\n'
    '  <line x1="51.2" y1="23.2" x2="55" y2="27" stroke="#4ecdc4" stroke-width="2.2" stroke-linecap="round"/>\n'
    '  <circle cx="43.5" cy="15.5" r="1.2" fill="white" opacity="0.5"/>\n'
    '</svg>\n'
)

out = pathlib.Path(r'D:\TD_Depot\Software\Lugwit_syncPlug\lugwit_insapp\trayapp\rez-package-source\view_pkl_tool\999.0\src\view_pkl_tool\icons\view_pkl_tool.svg')
out.write_text(svg, encoding='utf-8')

raw = out.read_bytes()[:4]
print('first 4 bytes hex:', raw.hex())
print('starts with BOM?', raw[:3] == b'\xef\xbb\xbf')
print('written', out.stat().st_size, 'bytes to', out)
