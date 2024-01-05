#!/bin/bash

cat extraction_logs/runlog_archive_extraction_service-202* | grep -v zstash | grep -v Begin | grep -v Conducting | grep -v Created
