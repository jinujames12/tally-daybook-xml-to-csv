#!/usr/bin/env python3
"""
XML_to_Daybook_voucherid_inventoryallocs_fixed.py

Fixed version of the original script. Root cause of the missing "Local Purchases 12 % -22800" line:

* The script previously removed *identical rows* at the end (deduped tuples), which collapsed
  distinct item-level accounting allocations that legitimately had the same ledger name and amount
  (e.g. two separate inventory items each allocating -22800 to the same ledger).

Fix applied:
* Removed the final tuple-based dedupe so that separate accounting allocations are preserved.
* Kept the existing de-duplication of XML elements (by element identity) inside
  `collect_ledger_entries_from_voucher` to avoid accidentally adding the *same* element twice.

If you prefer to still collapse truly duplicate lines while preserving separate allocations,
we can add a unique sequence/line index column (or a unique allocation id) instead. For now
this change preserves each allocation as a separate CSV row.

Outputs: voucher_id, vch_type, vch_key, date, gl_account, amount, narration
"""

from pathlib import Path
import xml.etree.ElementTree as ET
import csv
import re
import sys

ALLOWED_CONTROL_CODES = (9, 10, 13) # tab, newline, carriage return

# ---------- decoding + cleaning helpers ----------
def detect_decode_bytes(b: bytes) -> str:
    if b.startswith(b'\xef\xbb\xbf'):
        return b.decode('utf-8-sig', errors='replace')
    if b.startswith(b'\xff\xfe') or b.startswith(b'\xff\xfe\x00\x00'):
        try: return b.decode('utf-16', errors='replace')
        except: pass
    if b.startswith(b'\xfe\xff') or b.startswith(b'\x00\x00\xfe\xff'):
        try: return b.decode('utf-16-be', errors='replace')
        except: pass
    try:
        return b.decode('utf-8', errors='strict')
    except:
        try:
            return b.decode('utf-8', errors='replace')
        except:
            return b.decode('latin-1', errors='replace')


def remove_leading_before_angle(s: str) -> str:
    idx = s.find('<')
    return s if idx <= 0 else s[idx:]


def clean_xml_text(text: str) -> str:
    def repl_dec(m):
        try:
            cp = int(m.group(1))
        except:
            return ''
        return m.group(0) if cp in ALLOWED_CONTROL_CODES else ''
    text = re.sub(r'&#([0-9]+);', repl_dec, text)

    def repl_hex(m):
        try:
            cp = int(m.group(1), 16)
        except:
            return ''
        return m.group(0) if cp in ALLOWED_CONTROL_CODES else ''
    text = re.sub(r'&#x([0-9A-Fa-f]+);', repl_hex, text)

    out_chars = []
    for ch in text:
        code = ord(ch)
        if code >= 32 or code in ALLOWED_CONTROL_CODES:
            out_chars.append(ch)
        else:
            continue
    return ''.join(out_chars)

# ---------- XML parsing helpers ----------
def strip_tag(t):
    if t is None:
        return ''
    return t.split('}')[-1].upper()


def text_of_child(parent, candidates):
    for child in parent:
        if strip_tag(child.tag) in candidates:
            return (child.text or '').strip()
    return None


def find_text_anywhere(parent, candidate_tags):
    for elem in parent.iter():
        if strip_tag(elem.tag) in candidate_tags:
            return (elem.text or '').strip()
    return None


def normalize_amount(s):
    if s is None:
        return ''
    s = s.strip()
    if s == '':
        return ''
    s = s.replace(',', '')
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]
    s = re.sub(r'[^\d\.\-]', '', s)
    return s


def reformat_date(s):
    if s is None:
        return ''
    s = s.strip()
    if re.fullmatch(r'\d{8}', s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    m = re.search(r'(\d{1,2})[^\d](\d{1,2})[^\d](\d{2,4})', s)
    if m:
        a,b,c = m.groups()
        if len(a) == 4:
            y,mo,da = a, b.zfill(2), c.zfill(2)
        elif len(c) == 4:
            y,mo,da = c, a.zfill(2), b.zfill(2)
        else:
            yy = int(c)
            y = 2000 + yy if yy < 50 else 1900 + yy
            mo,da = b.zfill(2), a.zfill(2)
        return f"{y}-{mo}-{da}"
    return s

# --------- ledger collection (unchanged) ----------
def collect_ledger_entries_from_voucher(v):
    entries = []

    for child in v:
        t = strip_tag(child.tag)
        if 'LEDGERENTRIES' in t or 'ALLLEDGERENTRIES' in t:
            if any(strip_tag(c.tag) in ('LEDGERNAME','PARTYLEDGERNAME','NAME','AMOUNT') for c in child):
                entries.append(child)
            else:
                for sub in child:
                    if any(strip_tag(c.tag) in ('LEDGERNAME','PARTYLEDGERNAME','NAME','AMOUNT') for c in sub):
                        entries.append(sub)

    for child in v:
        t = strip_tag(child.tag)
        if 'ALLINVENTORYENTRIES' in t or 'ALLINVENTORYENTRIES.LIST' in t or t.endswith('ALLINVENTORYENTRIES.LIST'.upper()):
            for inv_sub in child:
                st = strip_tag(inv_sub.tag)
                if 'ACCOUNTINGALLOCATIONS' in st or 'ACCOUNTINGALLOCATIONS.LIST' in st:
                    if any(strip_tag(c.tag) in ('LEDGERNAME','PARTYLEDGERNAME','NAME','AMOUNT') for c in inv_sub):
                        entries.append(inv_sub)
                    else:
                        for acct in inv_sub:
                            if any(strip_tag(c.tag) in ('LEDGERNAME','PARTYLEDGERNAME','NAME','AMOUNT') for c in acct):
                                entries.append(acct)

    # dedupe by element identity (prevents the *same* Element object being added twice)
    seen = set(); unique=[]
    for e in entries:
        if id(e) not in seen:
            unique.append(e); seen.add(id(e))
    return unique


def extract_voucher_id(v):
    candidate_tags = ('REMOTEID','VOUCHERREMOTEID','GUID','VCHKEY','VOUCHERID','ID','UUID')
    vid = text_of_child(v, candidate_tags)
    if vid:
        return vid
    vid = find_text_anywhere(v, candidate_tags)
    if vid:
        return vid
    for k, val in (v.attrib.items() if hasattr(v, 'attrib') else []):
        if strip_tag(k) in candidate_tags:
            return str(val)
    return ''

# --- new helper: extract voucher type and vchkey ---
def extract_voucher_type_and_key(v):
    """
    Returns (vch_type, vch_key) trying attributes first, then child elements.
    """
    # try attributes first (most Tally exports place these as attributes on <VOUCHER>)
    vch_type = ''
    vch_key = ''
    for k, val in (v.attrib.items() if hasattr(v, 'attrib') else []):
        kname = strip_tag(k)
        if kname == 'VCHTYPE' and not vch_type:
            vch_type = str(val)
        elif kname == 'VCHKEY' and not vch_key:
            vch_key = str(val)
        elif kname == 'VCHKEY'.upper() and not vch_key:
            vch_key = str(val)

    # fallback: check for child elements containing these values
    if not vch_type:
        vch_type = text_of_child(v, ('VCHTYPE','VOUCHERTYPE','TYPE')) or ''
    if not vch_key:
        vch_key = text_of_child(v, ('VCHKEY','VOUCHERKEY','KEY')) or ''

    # final fallback: search anywhere under the voucher
    if not vch_type:
        vch_type = find_text_anywhere(v, ('VCHTYPE','VOUCHERTYPE','TYPE')) or ''
    if not vch_key:
        vch_key = find_text_anywhere(v, ('VCHKEY','VOUCHERKEY','KEY')) or ''

    return vch_type, vch_key


def parse_tally_daybook_from_string(xml_text: str, csv_path: Path):
    print("Parsing XML data...")
    root = ET.fromstring(xml_text)
    rows = []
    vouchers = [elem for elem in root.iter() if strip_tag(elem.tag) == 'VOUCHER' or strip_tag(elem.tag).endswith('VOUCHER')]
    if not vouchers:
        vouchers = [elem for elem in root.iter() if any(x in strip_tag(elem.tag) for x in ('DAYBOOK','DAYBOOKENTRY','VOUCHER'))]

    total_vouchers = len(vouchers)
    print(f"Found {total_vouchers} vouchers. Processing...")

    for i, v in enumerate(vouchers):
        if (i + 1) % 100 == 0 or (i + 1) == total_vouchers:
            print(f"Processed {i + 1}/{total_vouchers} vouchers...")
        
        voucher_id = extract_voucher_id(v)
        vch_type, vch_key = extract_voucher_type_and_key(v)  # <-- new extraction
        date_raw = text_of_child(v, ['DATE','VOUCHERDATE']) or ''
        narration = (text_of_child(v, ['NARRATION']) or '').replace('\n',' ').strip()
        date = reformat_date(date_raw)
        ledger_entries = collect_ledger_entries_from_voucher(v)

        if not ledger_entries:
            ledgername = text_of_child(v, ['LEDGERNAME','PARTYLEDGERNAME','NAME']) or ''
            amount = normalize_amount(text_of_child(v, ['AMOUNT']) or '')
            rows.append((voucher_id, vch_type, vch_key, date, ledgername, amount, narration))
        else:
            for le in ledger_entries:
                ledgername = text_of_child(le, ['LEDGERNAME','PARTYLEDGERNAME','NAME']) or ''
                amount = normalize_amount(text_of_child(le, ['AMOUNT']) or '')
                if ledgername or amount:
                    rows.append((voucher_id, vch_type, vch_key, date, ledgername, amount, narration))

    # NOTE: previously the script removed exact duplicate rows by tuple equality here which
    # caused multiple separate item-level allocations with identical ledger+amount to be
    # collapsed to a single row. We now preserve all rows as collected above so that each
    # inventory accounting allocation remains a separate CSV line.

    print("Writing data to CSV...")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # updated header to include vch_type and vch_key
        writer.writerow(['voucher_id','vch_type','vch_key','date','gl_account','amount','narration'])
        for voucher_id, vch_type, vch_key, date, ledger, amount, narration in rows:
            writer.writerow([voucher_id, vch_type, vch_key, date, ledger, amount, narration])

    return len(rows)

# ---------- main ----------
def main():
    try:
        
        print("=" * 60)
        print("James & James LLP")
        print("Tally Daybook -  XML to CSV Converter")
        print("=" * 60)
        print()
        
        input_path = input("Enter the path to the XML file: ")
        input_xml_path = Path(input_path)
        output_csv_path = input_xml_path.parent / (input_xml_path.stem + "_extracted.csv")
        cleaned_xml_path = input_xml_path.with_name(input_xml_path.stem + "_cleaned.xml")
        WRITE_CLEANED_XML = True

        if not input_xml_path.exists():
            print(f"ERROR: input file not found: {input_xml_path}")
            raise SystemExit(1)

        print("Reading XML file...")
        raw_bytes = input_xml_path.read_bytes()
        decoded = detect_decode_bytes(raw_bytes)
        stripped = remove_leading_before_angle(decoded)
        cleaned = clean_xml_text(stripped)

        if WRITE_CLEANED_XML:
            try:
                print("Writing cleaned XML...")
                cleaned_xml_path.write_text(cleaned, encoding='utf-8')
                print(f"Cleaned XML written to: {cleaned_xml_path}")
            except Exception as e:
                print("Warning: could not write cleaned XML:", e)

        count = parse_tally_daybook_from_string(cleaned, output_csv_path)
        print(f"\nSuccess — wrote {count} rows to: {output_csv_path}")

    except ET.ParseError as pe:
        print("\nXML parse error after cleaning:", pe)
        sys.exit(1)
    except Exception as e:
        print("\nERROR:", e)
        sys.exit(1)

if __name__ == '__main__':
    main()
