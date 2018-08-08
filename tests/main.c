/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#if _MSC_VER
#    pragma warning(disable : 4100) /* unreferenced formal parameter */
#    pragma warning(disable : 4204) /* non-constant aggregate initializer */
#    pragma warning(disable : 4221) /* initialization using address of automatic variable */
#endif

#include <aws/testing/aws_test_harness.h>

#include "packet_encoding_test.c"

static int run_tests(int argc, char *argv[]) {
    AWS_RUN_TEST_CASES(
        &mqtt_packet_puback,
        &mqtt_packet_pubrec,
        &mqtt_packet_pubrel,
        &mqtt_packet_pubcomp,
        &mqtt_packet_suback,
        &mqtt_packet_unsuback,
        &mqtt_packet_connect,
        &mqtt_packet_connack,
        &mqtt_packet_publish,
        &mqtt_packet_subscribe,
        &mqtt_packet_unsubscribe,
        &mqtt_packet_pingreq,
        &mqtt_packet_pingresp,
        &mqtt_packet_disconnect);
}

int main(int argc, char *argv[]) {
    int ret_val = run_tests(argc, argv);
    return ret_val;
}
