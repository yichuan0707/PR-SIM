PR-SIM is a simulator for distributed storage system. The architecture of PR-SIM is the same with DS-SIM(open soure on google codes), but we implement the simulator with Python, not Java.
And we make the following improvements:
(1) fully considered the correlated failures, not only machine failures caused by rack failure, but also the event like power outage.
(2) support failures cumulatives by many kinds of events, such as network breakdown, human errors, software errors, but the users must set the distribution of all the events first, and make sure their impacts(data lost, data unavailable or nothing).
(3) support reliability measurement for system scaling up or scaling down.
