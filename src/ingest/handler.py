import os, json, uuid, base64
from datetime import datetime, timezone
import boto3

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
BUCKET = os.environ["BUCKET_NAME"]
TABLE  = os.environ["SCHEMA_TABLE"]
 
def _parse_event(event):
    body = event.get("body", "")
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    try:
        data = json.loads(body)
    except Exception:
        return None, "Body must be valid JSON"
    # form_id can be in header or body
    form_id = (event.get("headers", {}) or {}).get("x-form-id") \
              or data.get("form_id")
    if not form_id:
        return None, "Missing form_id (header x-form-id or body.form_id)"
    return {"form_id": form_id, "data": data}, None

def _load_schema(form_id):
    table = ddb.Table(TABLE)
    resp = table.get_item(Key={"form_id": form_id})
    item = resp.get("Item")
    if not item:
        return None, f"Unknown form_id: {form_id}"
    return item, None

def _validate(payload, schema):
    data = payload["data"]
    fields = schema.get("fields", [])
    errors = []
    normalized = {}
    for f in fields:
        name = f["name"]
        ftype = f.get("type", "string")
        required = f.get("required", False)
        enum = f.get("values")
        val = data.get(name)
        if required and (val is None or (isinstance(val, str) and val.strip()=="")):
            errors.append(f"Missing required: {name}")
            continue
        if val is None:
            normalized[name] = None
            continue
        # basic type checks + coercion
        try:
            if ftype == "number":
                val = float(val)
            elif ftype == "boolean":
                if isinstance(val, bool):
                    pass
                elif str(val).lower() in ("true","1","yes"):
                    val = True
                elif str(val).lower() in ("false","0","no"):
                    val = False
                else:
                    raise ValueError("boolean")
            else:
                val = str(val)
        except Exception:
            errors.append(f"Bad type for {name}: expected {ftype}")
            continue
        if enum and val not in enum:
            errors.append(f"Invalid value for {name}: {val} not in {enum}")
        normalized[name] = val
    return (normalized, errors)

def _s3_key(prefix, form_id, ext):
    now = datetime.now(timezone.utc)
    return f"{prefix}/{form_id}/{now.year:04d}/{now.month:02d}/{now.day:02d}/{uuid.uuid4()}.{ext}"

def lambda_handler(event, context):
    payload, err = _parse_event(event)
    if err:
        return _resp(400, {"error": err})
    schema, err = _load_schema(payload["form_id"])
    if err:
        return _resp(400, {"error": err})
    normalized, errors = _validate(payload, schema)
    user_id = (event.get("headers", {}) or {}).get("x-user-id", "anonymous")

    # Always write raw
   raw_key = _s3_key("raw", payload["form_id"], "json")
    s3.put_object(
        Bucket=BUCKET,
        Key=raw_key,
        Body=json.dumps(payload["data"], ensure_ascii=False).encode("utf-8"),
        Metadata={
            "form_id": payload["form_id"],
            "user_id": user_id,
            "valid": "false" if errors else "true",
        },
        ContentType="application/json",
    )

    curated_key = None
    if not errors:
        # one-row CSV with headers in schema order
        headers = [f["name"] for f in schema.get("fields", [])]
        values = []
        for h in headers:
            v = normalized.get(h)
            if v is None:
                values.append("")
            else:
                s = str(v).replace('"','""')
                values.append(f'"{s}"')
        csv_bytes = (",".join(headers) + "\n" + ",".join(values) + "\n").encode("utf-8")
        curated_key = _s3_key("curated", payload["form_id"], "csv")
        s3.put_object(
            Bucket=BUCKET,
            Key=curated_key,
            Body=csv_bytes,
            Metadata={
                "form_id": payload["form_id"],
                "user_id": user_id,
                "valid": "true",
            },
            ContentType="text/csv",
        )

    return _resp(200, {
        "status": "ok" if not errors else "invalid",
        "errors": errors,
        "raw_key": raw_key,
        "curated_key": curated_key
    })

def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
        },
        "body": json.dumps(body),
    }
