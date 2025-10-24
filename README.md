# Data Intake Proto (API Gateway → Lambda → S3, DynamoDB schema)

Minimal intake pipeline:
- **UI:** any form (Amplify/Retool/etc.) POSTs JSON
- **API Gateway → Lambda:** validates against schema in **DynamoDB**
- **S3:** writes **raw** JSON and **curated** one-row CSV with metadata

## Deploy (AWS SAM)
```bash
# one-time
sam build
sam deploy --guided   # pick region, accept defaults for bucket/table names
```
Copy the **ApiUrl** from the outputs.

## Seed the schema (DynamoDB)
```bash
export TABLE_NAME=form-schemas      # or your chosen table from Outputs
python scripts/seed_schema.py
```

## Test (curl)
```bash
API="https://xxxx.execute-api.<region>.amazonaws.com/Prod/ingest"
curl -s -X POST "$API" \
  -H 'Content-Type: application/json' \
  -H 'x-form-id: project_tracker_v1' \
  -H 'x-user-id: hakeem' \
  -d '{"project_name":"Data Lake MVP","status":"Open","owner_email":"owner@example.com","estimate_days":3}'
```
Expected response:
```json
{"status":"ok","errors":[],"raw_key":"raw/project_tracker_v1/...json","curated_key":"curated/project_tracker_v1/...csv"}
```

## S3 layout
```
s3://<your-bucket>/
  raw/project_tracker_v1/YYYY/MM/DD/<uuid>.json
  curated/project_tracker_v1/YYYY/MM/DD/<uuid>.csv
```
Objects include metadata: `form_id`, `user_id`, `valid`.

## Hooking up a UI
- Amplify/Retool form → POST to the API URL with header `x-form-id: project_tracker_v1`.
- Body is plain JSON with the fields from your schema.

## Next steps
- Add **Glue Crawler** for `curated/` to expose Athena tables.
- Add **more schemas** by `PutItem` into the DynamoDB table (same shape as the seeded one).
52 - Extend `handler.py` with richer validation or `jsonschema`.
