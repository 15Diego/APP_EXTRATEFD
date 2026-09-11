from pathlib import Path
from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).parents[1] / "app.py")


def button(app, label):
    return next(b for b in app.button if b.label == label)


def test_demo_filters_search_reset_and_export():
    app = AppTest.from_file(APP, default_timeout=30).run()
    assert not app.exception
    button(app, "Explorar demonstração").click().run()
    assert not app.exception
    assert app.metric[0].value == "6"
    app.multiselect(key="f_operations").set_value(["Entrada"]).run()
    assert not app.exception
    assert app.metric[0].value == "3"
    app.text_input(key="f_search").set_value("[").run()
    assert not app.exception
    assert app.metric[0].value == "0"
    button(app, "Limpar filtros").click().run()
    assert app.metric[0].value == "6"
    button(app, "Preparar Excel").click().run()
    assert not app.exception
    assert app.session_state["export_bytes"][:2] == b"PK"
    app.multiselect(key="f_operations").set_value(["Saída"]).run()
    assert "export_bytes" not in app.session_state
    button(app, "Limpar sessão").click().run()
    assert "batch" not in app.session_state
    assert not app.exception
