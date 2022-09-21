from collector.backup_restore.postgres.postgres_factory import PostgresFactory


class BackupRestoreGroup(object):
    @staticmethod
    def get_factory(ptype):
        return {"postgres": PostgresFactory}.get(ptype)
