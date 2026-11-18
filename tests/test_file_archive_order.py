from decimal import Decimal
from betalens_db_manager.files import import_file, verify_file_cache
from test_manager import manager, table_definition


def test_persisted_mapping_order_does_not_change_wire_column_meaning(tmp_path):
    service=manager(tmp_path);service.registry.save(table_definition())
    source=tmp_path/'source.csv';source.write_text('ticker,day,price\na,2026-11-17,12.3\n')
    item=import_file(service,'sample_prices',source,{'ticker':'code','day':'business_date','price':'price'},targets=['postgres'])
    loaded=service.state.job(item['id'])
    assert list(loaded['plan']['file']['column_map'])==['day','price','ticker']
    verify_file_cache(service,loaded)
    rows,rejected=service._build_canonical(loaded)
    assert not rejected
    assert rows['sample_prices'][0]['code']=='a'
    assert rows['sample_prices'][0]['price']==Decimal('12.3')
