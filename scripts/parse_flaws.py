import re
import sys

sys.stdout.reconfigure(encoding='utf-8')

with open(r'C:\Users\RANADEEP\Downloads\flaws_and_remediation_guide.html', encoding='utf-8') as f:
    text = f.read()

pattern = re.compile(r'<div class="flaw-card"[^>]*id="(FLAW-ARCH-[^"]+)">(.*?)(?=<div class="flaw-card"|<footer>)', re.DOTALL)
for match in pattern.finditer(text):
    fid = match.group(1)
    body = match.group(2)
    clean = re.sub(r'<[^>]+>', ' ', body)
    clean = ' '.join(clean.split())
    print(f"=== {fid} ===")
    print(clean)
    print("\n" + "="*40 + "\n")
