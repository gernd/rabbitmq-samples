#!/bin/bash
docker run -d --hostname rabbitmq-samples --name rabbitmq-samples -p 8080:15672 -p 5672:5672 rabbitmq:3-management
