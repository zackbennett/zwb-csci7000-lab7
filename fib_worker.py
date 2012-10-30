#!/usr/bin/python

import pika, time, fib_pb2

qhost = open('queue_host').readline()
print qhost
connection = pika.BlockingConnection(pika.ConnectionParameters(host=qhost))

recvchan = connection.channel()
sendchan = connection.channel()

recvchan.queue_declare(queue='fib_to_compute', durable=True)
sendchan.queue_declare(queue='fib_from_compute', durable=True)

def compute_fib(n):
	if (n == 0):
		return 0
	i = 0
	n1 = 0
	n2 = 1
	while (i < n-1):
		n1,n2 = n2,n1+n2
		i = i+1
	return n2

def callback(ch, method, properties, body):
	print "[x] Received %r" % body
	gp_fiblist = fib_pb2.FibList()
	gp_fiblist.ParseFromString(body)
	fibs = gp_fiblist.fibs
	for f in fibs:
		f.response = str(compute_fib(f.n))
	response_body = gp_fiblist.SerializeToString()
	print "[x] Computed Answers"
	sendchan.basic_publish(exchange='',
					   routing_key='fib_from_compute',
					   body=response_body,
					   properties=pika.BasicProperties(content_type="text/plain", delivery_mode=1))
	print "[x] Done"
	ch.basic_ack(delivery_tag=method.delivery_tag)

recvchan.basic_consume(callback, queue='fib_to_compute')
recvchan.start_consuming()
