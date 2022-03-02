Developer Guide
---------------

Prerequisites
~~~~~~~~~~~~~

1. Linux OS (``esgf-forge/autocurator`` Anaconda package only available
   on Linux)
2. Miniconda or Anaconda installed
3. Cloned the repo or fork:

.. code-block:: bash

    $ git clone https://github.com/E3SM-Project/esgfpub.git
    $ cd esgfpub

Getting Started
~~~~~~~~~~~~~~~

Development Environment
^^^^^^^^^^^^^^^^^^^^^^^

This environment is intended for local development of future releases.
It does not include the latest stable release of ``esgfpub``.

1. Create the Anaconda development environment

   .. code:: bash

      $ cd esgfpub
      $ conda env create -f conda/dev.yml
      $ conda activate esgfpub_dev

2. Make changes to files in ``esgfpub/`` or ``warehouse/``
3. Test changes by following these instructions

   -  `esgfpub/`_
   -  `warehouse/`_

4. Commit and push

.. _esgfpub/: 2_esgfpub.rst#installation
.. _warehouse/: 3_warehouse.rst#installation