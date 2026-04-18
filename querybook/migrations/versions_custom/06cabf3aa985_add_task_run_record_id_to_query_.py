"""add task_run_record_id to query_execution

Revision ID: 06cabf3aa985
Revises: d75fae0b32ab
Create Date: 2026-04-18 16:32:53.477366

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '06cabf3aa985'
down_revision = 'd75fae0b32ab'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "query_execution",
        sa.Column("task_run_record_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "query_execution_task_run_record_id_fk",
        "query_execution",
        "task_run_record",
        ["task_run_record_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_query_execution_task_run_record_id",
        "query_execution",
        ["task_run_record_id"],
    )


def downgrade():
    op.drop_index("ix_query_execution_task_run_record_id", table_name="query_execution")
    op.drop_constraint(
        "query_execution_task_run_record_id_fk",
        "query_execution",
        type_="foreignkey",
    )
    op.drop_column("query_execution", "task_run_record_id")
