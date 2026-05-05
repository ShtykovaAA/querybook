# Querybook provisioning (Ansible)

Declarative provisioning for Querybook **environments**, **users / memberships**
(via the admin REST API) and **env-managed query engines + metastores**
(via env-config — see `querybook/server/lib/env_config/`).

## What changed

Query engines and metastores used to be created through the admin REST API
(`POST /ds/admin/query_engine/`). They are now defined in YAML and rendered
by ansible into two files that querybook reads at startup — no API calls,
no secrets in the application DB.

Two artefacts are rendered:

1. **`connections.yml`** — declarative `query_engines:` / `metastores:` lists
   without secrets. Secrets are referenced via `${VAR}` placeholders.
   Best mounted as a k8s ConfigMap (or any plain-text volume).

2. **`env-config.env`** — `KEY=value` lines with the secret env vars used
   by `${VAR}` substitution **plus** the `QUERYBOOK_CONNECTIONS_FILE` pointer
   so querybook knows where the YAML lives.
   Best mounted as a k8s Secret.

On startup querybook (`lib/env_config/loader.py`) parses the YAML, expands
`${VAR}` from `os.environ`, optionally merges with `QUERYBOOK_QUERY_ENGINE_*`
and `QUERYBOOK_METASTORE_*` env vars (env wins on name collision), and
runs `sync_env_to_db()` to create / update shadow rows in the application DB.

The role still uses the API for:
- admin login / first signup
- user accounts
- environment objects + member assignments

It no longer uses the API for:
- query engines (read-only via env-config)
- metastores (read-only via env-config)
- engine ↔ environment binding (env-config does it via the `environments` JSON field)

## Layout

```
ansible/
├── ansible.cfg
├── inventory.yml
├── playbook.yml
├── group_vars/all/
│   ├── main.yml          # base URL, admin credentials
│   ├── resources.yml     # environments, users, env-managed engines/metastores
│   └── secrets.yml       # user passwords, ${VAR} substitutions for engine configs
└── roles/querybook_provision/
    ├── defaults/main.yml
    ├── templates/
    │   └── querybook_env_config.env.j2
    └── tasks/
        ├── main.yml
        ├── env_config.yml         # NEW — renders the env-config file
        ├── admin_bootstrap.yml
        ├── user.yml
        └── environment.yml
```

## What the demo playbook does

1. Renders `/etc/querybook/env-config.env` (path overridable via
   `querybook_env_file_path`) with all `QUERYBOOK_QUERY_ENGINE_*` and
   `QUERYBOOK_METASTORE_*` entries plus any extra secret env vars.
2. Bootstraps an admin session (login, or first-signup-becomes-admin).
3. Ensures `analyst` user exists.
4. Ensures the `analytics` environment exists with `analyst` as a member.

The query engine `sqlite_demo` from `resources.yml` lands in the env-config
file. Querybook's `lib/env_config/db_sync.py` picks it up on startup and:
- creates a shadow row in `query_engine`
- binds it to the `analytics` environment (because of `environments: [analytics]`)
- ensures FK consistency for `query_execution`, `data_doc_data_cell_meta`, etc.

The playbook is idempotent — re-running it diffs the rendered file and only
nudges querybook to restart when the file actually changed.

## Wire the rendered files into your deployment

Ansible rendering is one half; the other half is making **both** files
visible to querybook's web/worker/scheduler processes:

- `connections.yml` must be readable at the path `QUERYBOOK_CONNECTIONS_FILE`
  points to (set automatically by the env-config file).
- `env-config.env` must be loaded as process env vars.

**docker-compose:**
```yaml
services:
  web:
    env_file: /etc/querybook/env-config.env
    volumes:
      - /etc/querybook/connections.yml:/etc/querybook/connections.yml:ro
  worker:
    env_file: /etc/querybook/env-config.env
    volumes:
      - /etc/querybook/connections.yml:/etc/querybook/connections.yml:ro
  scheduler:
    env_file: /etc/querybook/env-config.env
    volumes:
      - /etc/querybook/connections.yml:/etc/querybook/connections.yml:ro
```

**systemd:**
```ini
[Service]
EnvironmentFile=/etc/querybook/env-config.env
# connections.yml at the same path on disk — read directly
```

**Kubernetes:**
```sh
# Secret — env vars (sensitive)
kubectl create secret generic querybook-env-config \
    --from-env-file=/etc/querybook/env-config.env

# ConfigMap — connections.yml (declarative, no plaintext secrets)
kubectl create configmap querybook-connections \
    --from-file=connections.yml=/etc/querybook/connections.yml
```
Then in each Deployment:
```yaml
envFrom:
  - secretRef:
      name: querybook-env-config
volumeMounts:
  - name: connections
    mountPath: /etc/querybook
    readOnly: true
volumes:
  - name: connections
    configMap:
      name: querybook-connections
```

After the files are wired in, restart the three querybook processes —
`sync_env_to_db()` runs once per process at startup, parses
`connections.yml`, and creates / updates shadow rows.

## Run

```sh
cd ansible
ansible-playbook playbook.yml

# with a real vault:
ansible-vault encrypt group_vars/all/secrets.yml
ansible-playbook playbook.yml --ask-vault-password
```

## Secrets

Two kinds of secrets, both lived in `group_vars/all/secrets.yml`:

1. `querybook_user_password_secrets` — referenced by user entries via
   `password_secret: <key>` in `resources.yml`.

2. `querybook_env_extra_secrets` — plain `KEY=value` pairs that get rendered
   alongside the JSON env vars. Use them as `${KEY}` substitutions inside
   `connection_string` (querybook's loader interpolates at startup):

   ```yaml
   # secrets.yml (vault this)
   querybook_env_extra_secrets:
     QUERYBOOK_PROD_PG_PASSWORD: "real-password"
   ```

   ```yaml
   # resources.yml
   querybook_env_query_engines:
     - name: prod_pg
       executor_params:
         connection_string: "postgresql+psycopg2://user:${QUERYBOOK_PROD_PG_PASSWORD}@db:5432/prod"
   ```

   The rendered env file ends up containing both lines. Querybook loader
   substitutes the placeholder; the password never reaches `query_engine.executor_params`
   in the database (shadow row keeps it `{}`).

## Notes / limitations

- The rendered env-config file contains secrets in plain text. Restrict its
  permissions (template task uses `mode: 0600`), don't commit it.
- Querybook still needs a running web instance for users / environments
  provisioning (admin REST API).
- This playbook is **additive** — it creates/updates but does not remove
  resources missing from YAML. Removing a query engine from
  `querybook_env_query_engines` and re-running renders an env file without
  it; on querybook restart the shadow row gets soft-deleted (`deleted_at`
  set), and `DELETE /admin/env_config/orphans/engine/<id>/` finishes cleanup.
- `prod_*` runtime modes (uwsgi for web, plain celery worker/beat) all pick
  up the env file the same way — see `runweb.py` (module-level sync) and
  `tasks/all_tasks.py` (`@celeryd_init` + `@beat_init`).
