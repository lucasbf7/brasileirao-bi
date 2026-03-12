$env:GCP_PROJECT_ID = "brasileirao-bi-analytics"
$env:BQ_DATASET = "brasileirao_bi"
$env:GOOGLE_APPLICATION_CREDENTIALS = (Resolve-Path ".\secrets\brasileirao-bi-sa.json").Path
$env:BQ_LOCATION = "southamerica-east1"