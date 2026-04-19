"""add timeout to taskrunstatus enum

Revision ID: a8651298b692
Revises: 06cabf3aa985
Create Date: 2026-04-18 20:24:26.266780

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a8651298b692'
down_revision = '06cabf3aa985'
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE taskrunstatus ADD VALUE IF NOT EXISTS 'TIMEOUT'")


def downgrade():
    op.execute(
        "UPDATE task_run_record SET status = 'FAILURE' WHERE status = 'TIMEOUT'"
    )
    op.execute("ALTER TYPE taskrunstatus RENAME TO taskrunstatus_old")
    op.execute("CREATE TYPE taskrunstatus AS ENUM ('RUNNING', 'SUCCESS', 'FAILURE')")
    op.execute(
        "ALTER TABLE task_run_record ALTER COLUMN status TYPE taskrunstatus "
        "USING status::text::taskrunstatus"
    )
    op.execute("DROP TYPE taskrunstatus_old")
