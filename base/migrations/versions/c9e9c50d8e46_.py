"""empty message

Revision ID: c9e9c50d8e46
Revises: 
Create Date: 2024-02-08 10:02:48.451000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'c9e9c50d8e46'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('stocks_calculated',
                    sa.Column('index', sa.Integer(), autoincrement=True, nullable=False),
                    sa.Column('mpId', sa.Integer(), nullable=True),
                    sa.Column('calculationDate', sa.DateTime(), server_default=sa.text("TIMEZONE('utc-3', now())"), nullable=False),
                    sa.Column('warehouseName', sa.String(), nullable=False),
                    sa.Column('barcode', sa.String(), nullable=False),
                    sa.Column('userId', sa.Integer(), nullable=False),
                    sa.Column('supplierName', sa.String(), nullable=False),
                    sa.Column('costPriceN', sa.Float(), nullable=False),
                    sa.Column('costPriceS', sa.Float(), nullable=False),
                    sa.Column('stocksDate', sa.Date(), nullable=False),
                    sa.Column('quantity', sa.Integer(), nullable=False),
                    sa.Column('m2', sa.Float(), nullable=False),
                    sa.Column('priceS', sa.Integer(), nullable=False),
                    sa.Column('quantityToClient', sa.Integer(), nullable=False),
                    sa.Column('quantityFromClient', sa.Integer(), nullable=False),
                    sa.Column('m6', sa.Float(), nullable=False),
                    sa.Column('m9', sa.Float(), nullable=False),
                    sa.Column('saleCount', sa.Integer(), nullable=False),
                    sa.Column('sumSaleCount60Days', sa.Integer(), nullable=False),
                    sa.PrimaryKeyConstraint('index')
                    )

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('stocks_calculated')
    # ### end Alembic commands ###
