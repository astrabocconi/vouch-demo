from pricing import subtotal, discount, shipping


def test_subtotal():
    assert subtotal([(10.0, 2), (5.5, 1)]) == 25.5


def test_discount_known_code():
    assert discount(100, "WELCOME10") == 90.0


def test_discount_unknown_code():
    assert discount(100, "NOPE") == 100


def test_free_shipping_threshold():
    assert shipping(50) == 0.0
    assert shipping(49.99) == 4.90


def test_free_shipping_item_count():
    assert shipping(10, item_count=5) == 0.0
    assert shipping(10, item_count=4) == 4.90


def test_discount_vip30():
    assert discount(200, "VIP30") == 140.0


def test_discount_gift25():
    assert discount(200, "GIFT25") == 150.0


def test_shipping_gift_wrap():
    assert shipping(10, gift_wrap=True) == 7.40
    assert shipping(50, gift_wrap=True) == 2.50
