#!/bin/bash

# Print a summary of all test results

BROKER_HOST=localhost
CA_FILE=/home/carlos.sanvicente/mosquitto/certs/ca.crt
PROXY_HOST=localhost
PROXY_PORT=1080
PROXY_URI="socks5h://testuser:testpass@${PROXY_HOST}:${PROXY_PORT}"
EXECUTABLE=./bin/mqtt3_client_app/mqtt3_client_app

declare -a TEST_NAMES
declare -a TEST_RESULTS
declare -a TEST_CODES

run_case() {
	echo ""
	echo ""
	local test_title="$1"
	echo "===== $test_title ====="
	shift
	"$@"
	local status=$?
	TEST_NAMES+=("$test_title")
	TEST_RESULTS+=("$status")
	TEST_CODES+=("$status")
}

print_summary() {
	GREEN='\033[0;32m'
	RED='\033[0;31m'
	NC='\033[0m' # No Color
	echo "===================="
	echo "Test Summary:"
	pass_count=0
	fail_count=0
	for i in "${!TEST_NAMES[@]}"; do
		name="${TEST_NAMES[$i]}"
		result="${TEST_RESULTS[$i]}"
		if [ "$result" -eq 0 ]; then
			echo -e "${GREEN}[PASS]${NC} $name"
			((pass_count++))
		else
			echo -e "${RED}[FAIL]${NC} $name (exit code ${TEST_CODES[$i]})"
			((fail_count++))
		fi
	done
	echo "--------------------"
	echo "Total: $((pass_count+fail_count)), Passed: $pass_count, Failed: $fail_count"
	echo "===================="
}


# Test case functions
test_proxy_mqtt_unencrypted() {
	run_case "Proxy, MQTT, unencrypted, authenticated (1883)" \
		$EXECUTABLE --broker-host $BROKER_HOST --broker-port 1883 \
		--proxy "$PROXY_URI"
}

test_proxy_mqtt_encrypted() {
	run_case "Proxy, MQTT, encrypted, authenticated (8883)" \
		$EXECUTABLE --broker-host $BROKER_HOST --broker-port 8883 \
		--proxy "$PROXY_URI" \
		--ca-file $CA_FILE
}

test_proxy_mqtt_ws_unencrypted() {
	run_case "Proxy, MQTT over WS, unencrypted, authenticated (8080)" \
		$EXECUTABLE --broker-host $BROKER_HOST --broker-port 8080 \
		--proxy "$PROXY_URI" \
		--websocket --ws-path /mqtt
}

test_proxy_mqtt_ws_encrypted() {
	run_case "Proxy, MQTT over WS, encrypted, authenticated (8081)" \
		$EXECUTABLE --broker-host $BROKER_HOST --broker-port 8081 \
		--proxy "$PROXY_URI" \
		--websocket --ws-path /mqtt \
		--ca-file $CA_FILE
}

# Call all test cases
test_proxy_mqtt_unencrypted
test_proxy_mqtt_encrypted
test_proxy_mqtt_ws_unencrypted
test_proxy_mqtt_ws_encrypted

print_summary
