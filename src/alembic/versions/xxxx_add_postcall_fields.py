"""Add post-call processing fields"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "xxxx_add_postcall_fields"
down_revision = "previous_revision"
branch_labels = None
depends_on = None

def upgrade():
    op.add_column("interactions", sa.Column("priority", sa.String(length=50), nullable=True))
    op.add_column("interactions", sa.Column("llm_tokens_used", sa.Integer(), nullable=True))
    op.add_column("interactions", sa.Column("processing_status", sa.String(length=50), nullable=True))
    op.add_column("interactions", sa.Column("error_log", postgresql.JSONB(), nullable=True))

    op.add_column("leads", sa.Column("last_call_stage", sa.String(length=100), nullable=True))

def downgrade():
    op.drop_column("interactions", "priority")
    op.drop_column("interactions", "llm_tokens_used")
    op.drop_column("interactions", "processing_status")
    op.drop_column("interactions", "error_log")

    op.drop_column("leads", "last_call_stage")
