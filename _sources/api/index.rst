Reference
=============

Board Pin Methods
-----------------

.. table::
   :class: table-align-left

   ===========================================  =======================================================
    :meth:`.pin_read`, :meth:`.pin_write`        |read-write|
    :meth:`.pin_meta`                            |arrange|
    :meth:`.pin_download`, :meth:`.pin_upload`   |select|
    :meth:`.pin_versions`, TODO complete         |mutate|
    :meth:`.pin_list`                            |summarize|
    :meth:`.pin_search`                          |group_by|
   ===========================================  =======================================================

.. |read-write| replace:: Read and write objects to and from a board
.. |arrange| replace:: Retrieve metadata for a pin
.. |select| replace:: Upload and download files to and from a board
.. |mutate| replace:: List, delete, and prune pin versions
.. |summarize| replace:: List all pins
.. |group_by| replace:: Search for pins


Board Constructors
------------------

Boards abstract over different storage backends, making it easy to share data in a variety of ways.

.. list-table::
   :class: table-align-left

   * - :func:`.board_folder`, :func:`.board_local`
     - Use a local folder as a board
   * - :func:`.board_rsconnect`
     - Use RStudio Connect as a board
   * - :func:`.board_s3`
     - Use an S3 bucket as a board
   * - :func:`.board_gcs`
     - Use an Google Cloud Storage bucket as a board
   * - :func:`.board_azure`
     - Use an Azure Datalake storage container as a board.
   * - :func:`.board`
     - Generic board constructor


.. toctree::
   :maxdepth: 2

   constructors
   boards
   meta
