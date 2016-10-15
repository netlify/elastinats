# elastinats

Intended to take from nats subjects/groups and pump data at elastic search. It will create 3 special fields:

  - @timestamp - the unix timestamp
  - @raw_msg - the raw string coming in
  - @source - the subject from the message

  If the data coming in is JSON it will parse that and send that parsed version to elasticsearch.

  # subject/group
  If a group is specified it will subscibe that way, otherwise it will just subscribe to the subject. This is useful for scaling out the cluster.

  For instance if you have 3 subjects: `logs.file1`, `logs.file2`, `logs.file3`. To distribute the load across multiple boxes the configuration on each would be:

  ```
  {
   ...
     "subjects": [
        {
          "subject": "logs.*",
          "group": "shared"
        }
     ]
   ...
  }
  ```

# endpoint configuration

It is possible to specify the elasticsearch configuration per subject. If one isn't specified, the default endpoint is used.

