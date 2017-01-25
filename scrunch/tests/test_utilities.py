from scrunch.variables import validate_variable_url


def test_variable_url_validation():
    ds_url = 'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3/'
    assert not validate_variable_url(ds_url)

    var_url = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3/'
    )
    assert validate_variable_url(var_url)

    var_wo_trailing_slash = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3'
    )
    assert validate_variable_url(var_wo_trailing_slash)

    subvar_url = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3'
        '/subvariables/b4d10b49c385aa405756fbbf572649d3/'
    )
    assert validate_variable_url(subvar_url)

    subvar_wo_trailing_slash = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3'
        '/subvariables/b4d10b49c385aa405756fbbf572649d3'
    )
    assert validate_variable_url(subvar_wo_trailing_slash)

    var_catalog = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3/summary'
    )
    assert not validate_variable_url(var_catalog)
