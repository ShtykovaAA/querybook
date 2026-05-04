class EnvConfigError(Exception):
    pass


class EnvManagedReadOnlyError(EnvConfigError):
    """Raised when trying to mutate an env-managed query engine or metastore."""

    pass
