Captured integration test output (Maven + Surefire + log4j2). Logs are excluded from Apache RAT in root pom.xml.

Files:
- multitable-it-20260324-150531.log — first full capture
- multitable-it-20260324-153140.log — after pull 8c4325c (checkpoint IT fix)
- multitable-it-20260324-155755.log — after pull ee69b41 (#15 transaction begin path)
- multitable-it-20260324-161230.log — after pull 67fa6d6 (#16 StreamLoadSnapshot / transaction loader)
- multitable-it-20260324-163250.log — after pull 260e2c1 (#17 SharedTransactionCoordinator)
- multitable-it-20260324-184020.log — after pull e3973cd (#18; MultiTableTransactionITTest all passed)
- multitable-it-20260324-193511.log — after pull fde6e09 (#19 checkpoint ITs; 9 tests, 1 failure: testTransactionSpansTwoCheckpointsFails)
- multitable-it-20260324-204640.log — after pull 1b05f56 (#20 restart strategy fix; 9 tests all passed)
- multitable-tsp-pipeline-20260324-221340.log — full pipeline on fccd569: SDK `mvn test` (StreamLoadManagerMultiTableTest fails vs mock), TSP apply `hujietest1-4u-benchmark-03242212` / FE `172.26.95.98`, wait for ports, MultiTable IT with `docker --network host` → 11 tests BUILD SUCCESS
