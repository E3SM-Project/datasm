


# Note: it is assumed that each utility below receives a list of datasets upon which to process, and the commands are applied independently to each dataset

Warehouse Orchestrator Commands | Warehouse Assign Utility Commands
------------------------------- | ---------------------------------
warehouse state LOCK | warehouse_assign --lock
. | warehouse_assign --setstatus LOCK
warehouse state FREE | warehouse_assign --free
. | warehouse_assign --setstatus FREE
warehouse filesystem adddir leafname | warehouse_assign --adddir leafdir
. | warehouse_assign --setstatus ADDDIR:leafdir


