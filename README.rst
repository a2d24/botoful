Contributing
============

Contributions are welcome.

Getting started
---------------

To work on the botoful codebase, you'll want to clone the project locally
and install the required dependencies via `poetry <https://poetry.eustace.io>`_.

.. code-block:: bash

    $ git clone git@github.com:a2d24/botoful.git

Quickstart
----------

.. code-block:: python

    import boto3
    from botoful import Query

    client = boto3.client('dynamodb')

    result = Query(table='Cars') \
                .key(brand='BMW', model__begins_with='1 Series') \
                .attributes(['brand', 'year', 'model']) \
                .execute(client)

    print(result.items)

Documentation
-------------

See `www.botoful.com <https://www.botoful.com>`_
