(ns grubber.node
  (:require [zeromq.zmq :as zmq]
            [clojure.core.async :as async]))


(defn get-available-port [] "5555")

(defn transform [transformer]
  (fn [data] (transformer data)))

(defn collect [collector] (fn [data] nil))

(defn threaded-pipeline! [consumer emitter run threads]
  (let [input (async/chan 1)
        output (async/chan 1)]
    (async/go-loop [data (zmq/receive-str consumer)]
      (async/>! input data)
      (recur (zmq/receive-str consumer)))
    (for [_ (range 1 threads)]
      (async/go-loop [data (async/<! input)]
        (async/>! output (run data))
        (recur (async/<! input))))
    (async/go-loop [data (async/<! output)]
      (zmq/send-str emitter data)
      (recur (async/<! output)))))

(defn single-pipeline! [consumer emitter runner]
  (async/go-loop [data (zmq/receive-str consumer)]
    (zmq/send-str emitter (runner data))
    (recur (zmq/receive-str consumer))))

(defn run-node! [node context emit-sock consume-sock run]
  (let [port (get-available-port)]
    (with-open [emitter (zmq/socket context emit-sock)
                consumer (doto (zmq/socket context consume-sock)
                           (zmq/bind (str "tcp://*:" port)))]
      (for [dst (:out node)]
        (zmq/connect emitter (:url dst)))
      (if (> (:threads node) 1)
        (threaded-pipeline! consumer emitter run threaded-pipeline!)
        (single-pipeline! consumer emitter run)))
    port))

(def node-properties {:transform {:runner       #'transform
                                  :emit-sock    :push
                                  :consume-sock :pull}
                      :collect   {:runner       #'collect
                                  :consume-sock :pull}})

(defn run-grubber! [node context]
  (let [properties ((node :type) node-properties)
        runner (:runner properties)
        emit-sock (:emit-sock properties)
        consume-sock (:consume-sock properties)
        node-fn (:fn node)]
    (run-node! node context emit-sock consume-sock (runner node-fn))))
