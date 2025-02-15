# Instructions
- Create a .dlt folder in the working directory you that contains `filesystem_pipeline.py`
- Create these files: `secrets.toml`, and `config.toml`
- Enter this in secrets.toml```bash
  [destination.bigquery]
  location = "US" # Change to actual location
  [destination.bigquery.credentials]
  project_id="<Google-cloud-console-project-id-here>"
  private_key="<private-key-here>"
  client_email="<client-email-here>"
- The run `python filesystem_pipeline.py`
