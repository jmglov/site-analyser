(ns s3-log-parser)

(defn handler [event context]
  (prn {:msg "Invoked with event",
        :data {:event event}})
  {})
