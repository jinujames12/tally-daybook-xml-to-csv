#!/usr/bin/env python3
"""
XML_to_Daybook_voucherid_inventoryallocs_fixed_plusfields_ref.py

Adds voucher-level and item-level fields to CSV rows, including REFERENCE.
Outputs columns:
voucher_id, vch_type, vch_key, date, vouchernumber, reference, partyname,
gl_account, stockitemname, rate, actualqty, billedqty, amount, narration
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

# --------- ledger collection (enhanced: attach inventory context) ----------
def collect_ledger_entries_from_voucher(v):
    """
    Returns a list of dicts: {'elem': element_containing_LEDGERNAME/AMOUNT, 'inv': inventory_entry_or_None}
    inv is the ALLINVENTORYENTRIES.LIST element that this accounting allocation belongs to (if any).
    """
    entries = []

    # voucher-level ledger containers (direct children only)
    for child in v:
        t = strip_tag(child.tag)
        if 'LEDGERENTRIES' in t or 'ALLLEDGERENTRIES' in t:
            # If child itself contains LEDGERNAME/AMOUNT, treat as one entry (inv=None)
            if any(strip_tag(c.tag) in ('LEDGERNAME','PARTYLEDGERNAME','NAME','AMOUNT') for c in child):
                entries.append({'elem': child, 'inv': None})
            else:
                # else its direct LIST children may be individual entries
                for sub in child:
                    if any(strip_tag(c.tag) in ('LEDGERNAME','PARTYLEDGERNAME','NAME','AMOUNT') for c in sub):
                        entries.append({'elem': sub, 'inv': None})

    # inventory-level accounting allocations: attach inv context
    for child in v:
        t = strip_tag(child.tag)
        if 'ALLINVENTORYENTRIES' in t:
            # child is an inventory entry (inv_entry)
            for inv_sub in child:
                st = strip_tag(inv_sub.tag)
                if 'ACCOUNTINGALLOCATIONS' in st:
                    # inv_sub might itself contain ledger info
                    if any(strip_tag(c.tag) in ('LEDGERNAME','PARTYLEDGERNAME','NAME','AMOUNT') for c in inv_sub):
                        entries.append({'elem': inv_sub, 'inv': child})
                    else:
                        # check direct children under ACCOUNTINGALLOCATIONS for ledger allocations
                        for acct in inv_sub:
                            if any(strip_tag(c.tag) in ('LEDGERNAME','PARTYLEDGERNAME','NAME','AMOUNT') for c in acct):
                                entries.append({'elem': acct, 'inv': child})

    # dedupe by element identity (prevents same Element object being added twice)
    seen = set(); unique=[]
    for e in entries:
        objid = id(e['elem'])
        if objid not in seen:
            unique.append(e); seen.add(objid)
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

def extract_voucher_type_and_key(v):
    vch_type = ''
    vch_key = ''
    for k, val in (v.attrib.items() if hasattr(v, 'attrib') else []):
        kname = strip_tag(k)
        if kname == 'VCHTYPE' and not vch_type:
            vch_type = str(val)
        elif kname == 'VCHKEY' and not vch_key:
            vch_key = str(val)
    if not vch_type:
        vch_type = text_of_child(v, ('VCHTYPE','VOUCHERTYPE','TYPE')) or ''
    if not vch_key:
        vch_key = text_of_child(v, ('VCHKEY','VOUCHERKEY','KEY')) or ''
    if not vch_type:
        vch_type = find_text_anywhere(v, ('VCHTYPE','VOUCHERTYPE','TYPE')) or ''
    if not vch_key:
        vch_key = find_text_anywhere(v, ('VCHKEY','VOUCHERKEY','KEY')) or ''
    return vch_type, vch_key

def extract_inventory_fields(inv_elem):
    """
    Given ALLINVENTORYENTRIES.LIST element, return tuple:
    (stockitemname, rate, actualqty, billedqty)
    If any field missing -> empty string
    """
    if inv_elem is None:
        return ('', '', '', '')
    stockitem = text_of_child(inv_elem, ('STOCKITEMNAME','STOCKITEM','STOCKITEMNAME')) or ''
    rate = text_of_child(inv_elem, ('RATE',)) or ''
    actualqty = text_of_child(inv_elem, ('ACTUALQTY','ACTUALQTY')) or ''
    billedqty = text_of_child(inv_elem, ('BILLEDQTY','BILLEDQTY')) or ''
    return (stockitem, rate, actualqty, billedqty)

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
        vch_type, vch_key = extract_voucher_type_and_key(v)
        date_raw = text_of_child(v, ['DATE','VOUCHERDATE']) or ''
        narration = (text_of_child(v, ['NARRATION']) or '').replace('\n',' ').strip()
        date = reformat_date(date_raw)

        # voucher-level additional fields
        partyname = text_of_child(v, ('PARTYNAME','PARTYLEDGERNAME','PARTYMAILINGNAME')) or ''
        vouchernumber = text_of_child(v, ('VOUCHERNUMBER','VCHNUM','VOUCHERNO')) or ''
        reference = text_of_child(v, ('REFERENCE','REF')) or ''

        ledger_entries = collect_ledger_entries_from_voucher(v)

        if not ledger_entries:
            ledgername = text_of_child(v, ['LEDGERNAME','PARTYLEDGERNAME','NAME']) or ''
            amount = normalize_amount(text_of_child(v, ['AMOUNT']) or '')
            stockitem, rate, actualqty, billedqty = ('', '', '', '')
            rows.append((voucher_id, vch_type, vch_key, date, vouchernumber, reference, partyname,
                         ledgername, stockitem, rate, actualqty, billedqty, amount, narration))
        else:
            for item in ledger_entries:
                le = item['elem']
                inv = item['inv']  # inventory context element or None
                ledgername = text_of_child(le, ['LEDGERNAME','PARTYLEDGERNAME','NAME']) or ''
                amount = normalize_amount(text_of_child(le, ['AMOUNT']) or '')
                stockitem, rate, actualqty, billedqty = extract_inventory_fields(inv)
                # If le's amount empty, try inventory amount.
                if not amount and inv is not None:
                    amount = normalize_amount(text_of_child(inv, ['AMOUNT']) or '')
                rows.append((voucher_id, vch_type, vch_key, date, vouchernumber, reference, partyname,
                             ledgername, stockitem, rate, actualqty, billedqty, amount, narration))

    print("Writing data to CSV...")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'voucher_id','vch_type','vch_key','date','vouchernumber','reference','partyname',
            'gl_account','stockitemname','rate','actualqty','billedqty','amount','narration'
        ])
        for r in rows:
            writer.writerow(r)

    return len(rows)

# ---------- main ----------
def main():
    try:
        
        print("=" * 60)
        print("James & James LLP")
        print("Tally Daybook - XML to CSV Converter (with item fields + reference)")
        print("=" * 60)
        print()
        
        input_path = input("Enter the path to the XML file: ")
        input_xml_path = Path(input_path)
        output_csv_path = input_xml_path.parent / (input_xml_path.stem + "_extracted_with_items_and_ref.csv")
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
