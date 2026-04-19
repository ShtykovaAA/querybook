"""add attempt and parent to task_run_record

Revision ID: 5110ff78837e
Revises: a8651298b692
Create Date: 2026-04-18 20:29:58.889760

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5110ff78837e'
down_revision = 'a8651298b692'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "task_run_record",
        sa.Column(
            "attempt",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
    )
    op.add_column(
        "task_run_record",
        sa.Column("parent_run_record_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "task_run_record_parent_id_fk",
        "task_run_record",
        "task_run_record",
        ["parent_run_record_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_task_run_record_parent_run_record_id",
        "task_run_record",
        ["parent_run_record_id"],
    )


def downgrade():
    op.drop_index(
        "ix_task_run_record_parent_run_record_id", table_name="task_run_record"
    )
    op.drop_constraint(
        "task_run_record_parent_id_fk", "task_run_record", type_="foreignkey"
    )
    op.drop_column("task_run_record", "parent_run_record_id")
    op.drop_column("task_run_record", "attempt")
