"""add main_connection_string and use_main_connection

Revision ID: b3f7d2c4e918
Revises: 5110ff78837e
Create Date: 2026-05-30 18:09:58.794080

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b3f7d2c4e918'
down_revision = '5110ff78837e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "query_engine",
        sa.Column("main_connection_string", sa.Text(), nullable=True),
    )
    op.add_column(
        "query_execution",
        sa.Column(
            "use_main_connection",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade():
    op.drop_column("query_execution", "use_main_connection")
    op.drop_column("query_engine", "main_connection_string")
