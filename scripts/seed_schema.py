import boto3, os, json

TABLE = os.environ.get("TABLE_NAME", "form-schemas")
ddb = boto3.resource("dynamodb").Table(TABLE)

schema = {
  "form_id": "project_tracker_v1",
  "fields": [
    {"name": "project_name", "type": "string", "required": True},
    {"name": "status", "type": "enum", "values": ["Open","Closed","Blocked"], "required": True},
    {"name": "owner_email", "type": "string", "required": False},
    {"name": "estimate_days", "type": "number", "required": False}
  ],
  "created_by": "admin@example.com",
  "version": "1.0"
}

if __name__ == "__main__":
    ddb.put_item(Item=schema)
    print("Seeded schema:", json.dumps(schema, indent=2))
