#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

check() {
    local label="$1"
    local expected="$2"
    local actual="$3"

    if [ "$actual" == "$expected" ]; then
        echo -e "${GREEN}PASS: $label${NC}"
        ((PASS++))
    else
        echo -e "${RED}FAIL: $label${NC}"
        echo -e "  expected: '$expected'"
        echo -e "  got:      '$actual'"
        ((FAIL++))
    fi
}

PORT=6369
CLI="redis-cli -p $PORT"

echo ""
echo "========================================"
echo "  GoRedis Full Command Battery Test"
echo "========================================"
echo ""

# ----------------------------------------
# PING
# ----------------------------------------
echo -e "${YELLOW}--- PING ---${NC}"

check "PING" "PONG" "$($CLI PING)"
check "PING with message" "hello" "$($CLI PING hello)"

# ----------------------------------------
# ECHO
# ----------------------------------------
echo -e "${YELLOW}--- ECHO ---${NC}"

check "ECHO" "hello world" "$($CLI ECHO "hello world")"

# ----------------------------------------
# SET / GET
# ----------------------------------------
echo -e "${YELLOW}--- SET / GET ---${NC}"

$CLI SET name "GoRedis" > /dev/null
check "SET/GET string" "GoRedis" "$($CLI GET name)"

$CLI SET number 42 > /dev/null
check "SET/GET number" "42" "$($CLI GET number)"

check "GET missing key" "" "$($CLI GET doesnotexist)"

# ----------------------------------------
# DEL
# ----------------------------------------
echo -e "${YELLOW}--- DEL ---${NC}"

$CLI SET to_delete "gone" > /dev/null
check "DEL existing key" "1" "$($CLI DEL to_delete)"
check "DEL already deleted" "0" "$($CLI DEL to_delete)"
check "DEL multiple keys" "2" "$($CLI DEL name number)"

# ----------------------------------------
# INCR
# ----------------------------------------
echo -e "${YELLOW}--- INCR ---${NC}"

$CLI DEL counter > /dev/null
check "INCR new key" "1" "$($CLI INCR counter)"
check "INCR existing key" "2" "$($CLI INCR counter)"
check "INCR existing key again" "3" "$($CLI INCR counter)"

$CLI SET badval "notanumber" > /dev/null
RES=$($CLI INCR badval)
if [[ "$RES" == *"ERR"* ]]; then
    echo -e "${GREEN}PASS: INCR on non-integer returns error${NC}"
    ((PASS++))
else
    echo -e "${RED}FAIL: INCR on non-integer should return error, got: $RES${NC}"
    ((FAIL++))
fi

# ----------------------------------------
# TTL / Expiration
# ----------------------------------------
echo -e "${YELLOW}--- TTL / Expiration ---${NC}"

$CLI SET permanent "value" > /dev/null
check "TTL no expiry" "-1" "$($CLI TTL permanent)"
check "TTL missing key" "-2" "$($CLI TTL ghostkey)"

$CLI SET temp "value" EX 5 > /dev/null
sleep 1
check "TTL after 1s" "3" "$($CLI TTL temp)"

sleep 6 
check "GET after expiry" "" "$($CLI GET temp)"
check "TTL after expiry" "-2" "$($CLI TTL temp)"

# ----------------------------------------
# LPUSH / RPUSH / LRANGE
# ----------------------------------------
echo -e "${YELLOW}--- List Commands ---${NC}"

$CLI DEL mylist > /dev/null

check "LPUSH single" "1" "$($CLI LPUSH mylist a)"
check "LPUSH multiple" "3" "$($CLI LPUSH mylist b c)"
check "RPUSH single" "4" "$($CLI RPUSH mylist d)"

RES=$($CLI LRANGE mylist 0 -1)
# after LPUSH c b a then RPUSH d — order is: c b a d
if [[ "$RES" == *"c"* && "$RES" == *"a"* && "$RES" == *"d"* ]]; then
    echo -e "${GREEN}PASS: LRANGE full list${NC}"
    ((PASS++))
else
    echo -e "${RED}FAIL: LRANGE full list, got: $RES${NC}"
    ((FAIL++))
fi

# ----------------------------------------
# LPOP / RPOP
# ----------------------------------------
echo -e "${YELLOW}--- LPOP / RPOP ---${NC}"

$CLI DEL poplist > /dev/null
$CLI RPUSH poplist one two three > /dev/null

check "LPOP" "one" "$($CLI LPOP poplist)"
check "RPOP" "three" "$($CLI RPOP poplist)"
check "LPOP empty key" "" "$($CLI LPOP doesnotexist)"
check "RPOP empty key" "" "$($CLI RPOP doesnotexist)"

# ----------------------------------------
# AOF Persistence
# ----------------------------------------
echo -e "${YELLOW}--- AOF Persistence ---${NC}"

$CLI SET persist_key "i_survived" > /dev/null
sleep 1 # give background fsync time to flush

# Note: to fully test this you need to restart the server and check
# the key still exists. This just confirms the key was set correctly.
check "AOF write survives" "i_survived" "$($CLI GET persist_key)"

# ----------------------------------------
# PUBLISH / SUBSCRIBE (basic smoke test)
# ----------------------------------------
echo -e "${YELLOW}--- PUBLISH ---${NC}"

# subscribe in background, capture output
(redis-cli -p $PORT SUBSCRIBE testchannel > /tmp/sub_output.txt 2>&1) &
SUB_PID=$!
sleep 0.5

# publish a message
RES=$($CLI PUBLISH testchannel "hello")
check "PUBLISH delivers to subscriber" "1" "$RES"

sleep 0.5
kill $SUB_PID 2>/dev/null
wait $SUB_PID 2>/dev/null

if grep -q "hello" /tmp/sub_output.txt; then
    echo -e "${GREEN}PASS: Subscriber received message${NC}"
    ((PASS++))
else
    echo -e "${RED}FAIL: Subscriber did not receive message${NC}"
    ((FAIL++))
fi

# ----------------------------------------
# WRONGTYPE errors
# ----------------------------------------
echo -e "${YELLOW}--- Type Safety ---${NC}"

$CLI SET strkey "hello" > /dev/null
RES=$($CLI LPUSH strkey value)
if [[ "$RES" == *"WRONGTYPE"* ]]; then
    echo -e "${GREEN}PASS: LPUSH on string key returns WRONGTYPE${NC}"
    ((PASS++))
else
    echo -e "${RED}FAIL: LPUSH on string key should return WRONGTYPE, got: $RES${NC}"
    ((FAIL++))
fi

# ----------------------------------------
# Summary
# ----------------------------------------
echo ""
echo "========================================"
TOTAL=$((PASS + FAIL))
echo "  Results: $PASS passed, $FAIL failed out of $TOTAL tests"
echo "========================================"
echo ""