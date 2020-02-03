(ns grubber.node
  (:require [zeromq.zmq :as zmq]
            [clojure.core.async :as async]
            [utils.core :as utils]))

(defn end-of-stream? [data]
  (= data :end-of-stream))

;;;;;;;

(defn get-available-port [] (utils/get-free-port))

(defn transform [transformer]
  (fn [data] (transformer data)))

(defn collect [collector] (fn [data] nil))

(defn threaded-pipeline! [consumer emitter run threads]
  (let [input (async/chan 1)
        output (async/chan 1)]

    (async/go-loop [data (zmq/receive-str consumer)]
      (async/>! input data)
      (or (end-of-stream? data)
          (recur (zmq/receive-str consumer))))

    ;; TODO: on :end-of-stream all co-routines should be closed properly
    (for [_ (range 0 threads)]
      (async/go-loop [data (async/<! input)]
        (async/>! output (run data))
        (or (and (end-of-stream? data)
                 (for [_ (range 0 (dec threads))]
                   (do
                     (async/>! input data)
                     true)))                                ;; TODO check if this solution closes threads properly
            (recur (async/<! input)))))

    (async/go-loop [data (async/<! output)]
      (zmq/send-str emitter data)
      (or (end-of-stream? data)
          (recur (async/<! output))))))

(defn single-pipeline! [consumer emitter runner]
  (async/go-loop [data (zmq/receive-str consumer)]
    (or (end-of-stream? data)
        (do
          (zmq/send-str emitter (runner data))
          (recur (zmq/receive-str consumer))))))

(defn run-node! [node context emit-sock consume-sock run]
  (utils/debug "node")
  (utils/debug node)
  (let [port (get-available-port)]
    (utils/debug emit-sock)
    (utils/debug consume-sock)
    (let [emitter (zmq/socket context emit-sock)
          consumer (doto (zmq/socket context consume-sock)
                     (zmq/bind (str "tcp://*:" port)))]
      (for [dst (:out node)]
        (zmq/connect emitter dst))
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
  ;; node contains :type :fn and :out
  (let [properties ((:type node) node-properties)
        runner (:runner properties)
        emit-sock (:emit-sock properties)
        consume-sock (:consume-sock properties)
        node-fn (:fn node)]
    (run-node! node @context emit-sock consume-sock (runner node-fn))))
