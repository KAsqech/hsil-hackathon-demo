# oecd_discover_codes.py (fixed for the new OECD SDMX API)
import csv, io, json, os, time, urllib.request, xml.etree.ElementTree as ET

BASE = "https://sdmx.oecd.org/public/rest"
OUTDIR = "oecd_codelists"
os.makedirs(OUTDIR, exist_ok=True)

DEFAULT_HEADERS = {
    # Prefer SDMX structure JSON; fall back to XML if the server doesn’t support JSON for this call
    "User-Agent": "OECD-Discovery/1.1",
    "Accept": (
        "application/vnd.sdmx.structure+json;version=1.0.0, "
        "application/vnd.sdmx.structure+xml;version=2.1;q=0.9, "
        "application/xml;q=0.8, text/xml;q=0.7, */*;q=0.5"
    ),
}

def get(url, accept=None):
    headers = DEFAULT_HEADERS.copy()
    if accept:
        headers["Accept"] = accept
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as r:
        ctype = r.headers.get("Content-Type", "")
        body = r.read()
    return body, ctype

def parse_dataflow_json(payload):
    """Parse SDMX-Structure JSON dataflow/all into a list of dicts."""
    obj = json.loads(payload.decode("utf-8"))
    flows = []

    # SDMX-JSON structure often nests under 'dataflows' -> 'dataflow'
    # Shapes differ; handle common variants
    df = None
    if "dataflows" in obj and isinstance(obj["dataflows"], dict):
        df = obj["dataflows"].get("dataflow")
    if df is None and "structure" in obj and isinstance(obj["structure"], dict):
        df = obj["structure"].get("dataflows") or obj["structure"].get("dataflow")

    if isinstance(df, dict):
        items = df.values()
    elif isinstance(df, list):
        items = df
    else:
        items = []

    for item in items:
        # Common fields across SDMX implementations
        fid = item.get("id") or item.get("ID") or item.get("name")
        agency = item.get("agencyID") or item.get("agencyId") or item.get("agency")
        version = item.get("version") or ""
        name = None
        # Names can be multilingual dicts or strings
        nm = item.get("name") or item.get("Name")
        if isinstance(nm, dict):
            name = nm.get("en") or next(iter(nm.values()), None)
        elif isinstance(nm, list):
            # list of {value, lang}
            name = next((n.get("value") for n in nm if isinstance(n, dict) and n.get("lang") == "en"), None) or str(nm)
        else:
            name = nm or fid
        if fid and agency:
            flows.append({"id": fid, "agency": agency, "version": version, "title": name})
    return flows

def parse_dataflow_xml(payload):
    """Parse SDMX-Structure XML dataflow/all into a list of dicts."""
    # Namespaces commonly used by SDMX 2.1 structure messages
    NS = {
        "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
        "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
        "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    }
    root = ET.fromstring(payload)
    flows = []
    for df in root.findall(".//str:Dataflow", NS):
        fid = df.get("id")
        agency = df.get("agencyID") or df.get("agencyId") or df.get("agency")
        version = df.get("version") or ""
        # pick English name if available
        title = None
        for name in df.findall("./com:Name", NS):
            if (name.get("{http://www.w3.org/XML/1998/namespace}lang") or "").lower() == "en":
                title = name.text
                break
        if not title:
            # fallback to first available
            first = df.find("./com:Name", NS)
            title = first.text if first is not None else fid
        if fid and agency:
            flows.append({"id": fid, "agency": agency, "version": version, "title": title})
    return flows

def list_dataflows():
    # Per docs: https://sdmx.oecd.org/public/rest/dataflow/all
    body, ctype = get(f"{BASE}/dataflow/all")
    ctype_l = ctype.lower()
    if "json" in ctype_l:
        flows = parse_dataflow_json(body)
    else:
        # Some servers return XML by default
        flows = parse_dataflow_xml(body)
    # Deduplicate by (agency,id,version)
    uniq = {}
    for f in flows:
        key = (f["agency"], f["id"], f["version"])
        uniq[key] = f
    return list(uniq.values())

def fetch_structure(agency, dataflow_id, version=""):
    """
    Query structure for a dataflow (dimensions, codelists, etc.).
    Docs example:
      {BASE}/dataflow/{agency}/{dataflow}/{version}?references=all&detail=referencepartial
    Leaving version empty uses the latest version.
    """
    url = f"{BASE}/dataflow/{agency}/{dataflow_id}/{version}".rstrip("/")
    url += "?references=all&detail=referencepartial"
    # Request structure JSON if available; fall back to XML
    try:
        body, ctype = get(url, accept=DEFAULT_HEADERS["Accept"])
        if "json" in ctype.lower():
            return json.loads(body.decode("utf-8"))
        else:
            # If you prefer XML saved as text
            return {"__format__": "xml", "__raw__": body.decode("utf-8")}
    except Exception as e:
        raise RuntimeError(f"structure fetch failed: {e}")

def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    flows = list_dataflows()
    print(f"Found {len(flows)} dataflows. Writing to {OUTDIR}/index.json …")
    save_json(os.path.join(OUTDIR, "index.json"), flows)

    for i, f in enumerate(sorted(flows, key=lambda x: (x['agency'], x['id']))):
        dsid = f["id"]; agency = f["agency"]; ver = f["version"]
        title = f.get("title", dsid)
        try:
            print(f"[{i+1}/{len(flows)}] {agency}:{dsid} (v{ver or 'latest'}) – {title} …")
            struct = fetch_structure(agency, dsid, ver)
            # Save either JSON structure or raw XML (wrapped)
            out_path = os.path.join(OUTDIR, f"{agency}_{dsid}_{ver or 'latest'}.structure.json")
            save_json(out_path, struct)
            time.sleep(0.3)  # gentle throttle
        except Exception as e:
            print(f"Failed {agency}:{dsid}: {e}")

if __name__ == "__main__":
    main()
