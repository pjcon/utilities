#!/bin/bash
# Appends argument to JoinJobRecords to modify which procedure is called during testing
sudo mysql -u root <<< "USE clientdb; CALL JoinJobRecords$1"
