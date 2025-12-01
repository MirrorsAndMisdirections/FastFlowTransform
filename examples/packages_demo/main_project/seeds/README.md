# Seeds directory

`seed_users.csv` provides the CRM users source for the packages demo.
It is materialized as `seed_users` by `fft seed`, then referenced in
`sources.yml` as:

```yaml
sources:
  - name: crm
    tables:
      - name: users
        identifier: seed_users
