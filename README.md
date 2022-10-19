# FHIR to Postgres

A simple python script to scrape the FHIR schemas from http://hl7.org/fhir and transform them into Postgres tables. This approach creates a column for each field instead of stuffing the JSON representation into a column. By doing this you can better leverage the community of tools built on Postgres like [Postgrest](https://postgrest.org/en/stable/), [Supabase](https://github.com/supabase/supabase) or [Hasura](https://github.com/hasura/graphql-engine). References are handled through foreign keys and in cases where a reference references more than one resource we create a polymorphic table. 

# Quickstart

Setup your virtual env of choice.
```
pip install -r requirements.txt

python --sql-file output.sql --resources "CarePlan" "Patient" "Communication" "Practitioner" "Identifier" "CodeableConcept" "Observation" "Consent" "Goal" "HumanName" "ContactPoint"
```

If you'd like to generate tables for every FHIR resource exclude the `--resources` flag. Otherwise specify which subset you'd like to generate tables for. 