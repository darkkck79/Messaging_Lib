#!/bin/bash
cd /d/projects/Messaging_Lib
gradlew.bat :messaging-core:test --tests "com.messaging.internal.TypedChannelTest" -i
