setup-emulator:
	curl -s "${SPANNER_EMULATOR_HOST_REST}/v1/projects/${SPANNER_PROJECT_ID}/instances" --data '{"instanceId": "'${SPANNER_INSTANCE_ID}'"}'
	curl -s "${SPANNER_EMULATOR_HOST_REST}/v1/projects/${SPANNER_PROJECT_ID}/instances/${SPANNER_INSTANCE_ID}/databases" --data '{"createStatement": "CREATE DATABASE `'${SPANNER_DATABASE_ID}'`"}'
