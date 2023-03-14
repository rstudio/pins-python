Reference
=============

Board Pin Methods
-----------------

.. table::
   :class: table-align-left

   ===========================================  =======================================================
    :meth:`.pin_read`, :meth:`.pin_write`        |read-write|
    :meth:`.pin_meta`                            |meta|
    :meth:`.pin_download`, :meth:`.pin_upload`   |download|
    :meth:`.pin_versions`                        |versions|
    :meth:`.pin_list`                            |list|
    :meth:`.pin_search`                          |search|
   ===========================================  =======================================================

.. |read-write| replace:: Read and write objects to and from a board
.. |meta| replace:: Retrieve metadata for a pin
.. |download| replace:: Upload and download files to and from a board
.. |versions| replace:: List, delete, and prune pin versions
.. |list| replace:: List all pins
.. |search| replace:: Search for pins


Board Constructors
------------------

Boards abstract over different storage backends, making it easy to share data in a variety of ways.

.. list-table::
   :class: table-align-left

   * - :func:`.board_azure`
     - Use an Azure storage container as a board
   * - :func:`.board_folder`, :func:`.board_local`, :func:`.board_temp`
     - Use a local folder as a board
   * - :func:`.board_connect`
     - Use Posit Connect as a board
   * - :func:`.board_rsconnect`
     - Alias for Posit Connect board (it was formerly called RStudio Connect)
   * - :func:`.board_s3`
     - Use an S3 bucket as a board
   * - :func:`.board_gcs`
     - Use a Google Cloud Storage bucket as a board.
   * - :func:`.board_url`
     - Use a dictionary of URLs as a board
   * - :func:`.board`
     - Generic board constructor


.. toctree::
   :maxdepth: 2

   constructors
   boards
   meta
