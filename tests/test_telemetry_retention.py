from datetime import UTC, datetime

import pytest

from scripts import telemetry_retention


class Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class Connection:
    def __init__(self, selected=3, deleted=3):
        self.selected = selected
        self.deleted = deleted
        self.commands = []

    def transaction(self):
        return Transaction()

    async def execute(self, query, *_args):
        self.commands.append(query)
        if query == telemetry_retention.ROLLUP_SQL:
            return "INSERT 0 2"
        return "OK"

    async def fetchval(self, query, *_args):
        self.commands.append(query)
        if "retention_batch" in query and "removed" not in query:
            return self.selected
        return self.deleted


@pytest.mark.asyncio
async def test_execute_batch_rolls_up_before_deleting():
    connection = Connection()

    result = await telemetry_retention.execute_batch(
        connection, datetime.now(UTC), batch_size=100
    )

    rollup_index = connection.commands.index(telemetry_retention.ROLLUP_SQL)
    delete_index = next(
        index for index, query in enumerate(connection.commands) if "DELETE FROM" in query
    )
    assert rollup_index < delete_index
    assert result == {"selected_events": 3, "rollup_rows": 2, "deleted_events": 3}


@pytest.mark.asyncio
async def test_execute_batch_rejects_delete_count_mismatch():
    with pytest.raises(RuntimeError, match="selected 3 events but deleted 2"):
        await telemetry_retention.execute_batch(
            Connection(selected=3, deleted=2), datetime.now(UTC), batch_size=100
        )
