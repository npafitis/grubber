(ns grubber.node
  (:require [zeromq.zmq :as zmq]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [clojure.java.shell :as shell]
            [utils.core :as utils]))

(defn end-of-stream? [data]
  (= data :end-of-stream))

(defrecord NodeCtx [node node-properties threads acc])

(def node-context-map (atom {}))

(defn get-node [port]
  (:node (@node-context-map port)))

(defn get-acc [port]
  (:acc (@node-context-map port)))

(defn get-node-properties [port]
  (:node-properties (@node-context-map port)))

(defn get-node-emit-sock [port]
  (:emit-sock (get-node-properties port)))

(defn get-threads [port]
  (:threads (get-node port)))

(defn get-node-fn [port]
  (:fn (get-node port)))

(defn get-runner [port emitter]
  (let [node-properties (get-node-properties port)
        runner (:runner node-properties)
        node-fn (get-node-fn port)]
    (runner node-fn port emitter)))

(defn update-node-context [port
                           ^NodeCtx node-ctx]
  (swap! node-context-map #(assoc % port node-ctx)))

(defn update-node-acc [port
                       fun]
  (swap! node-context-map
         (fn [node-context]
           (update-in node-context [port :acc] fun))))
;;;;;;;

(defn get-available-port [] (utils/get-free-port))

(defn nmap [mapper port emitter]
  (fn [data] (cond (end-of-stream? data) (doseq [_ (:out (get-node port))]
                                           (utils/write-sock emitter :end-of-stream))
                   :else (utils/write-sock emitter ((eval mapper) data)))))

(defn nreduce [reducer port emitter]
  (fn [data]
    (log/info "Reducing" data "with" (get-acc port))
    (cond (end-of-stream? data) (do
                                  (utils/write-sock emitter (get-acc port))
                                  (utils/write-sock emitter :end-of-stream))
          :else (update-node-acc port #((eval reducer) % data)))))

(defn nshell [script _ emitter]
  (fn [data]
    (log/info "Executing" script)
    (cond (end-of-stream? data) (utils/write-sock emitter :end-of-stream)
          :else (let [exec-res (:out (shell/sh "bash" "-c" script (str data)))]
                  (or (nil? exec-res)
                      (utils/write-sock emitter exec-res))))))

(defn emitter-connect! [emitter port]
  (let [outs (:out (get-node port))]
    (doseq [out outs]
      (or (nil? out)
          (do
            (log/info "Emitter connecting to " out)
            (zmq/connect emitter (str "tcp://" out)))))))

(defn single-pipeline! [context input-chan port]
  (log/info "Starting single-threaded pipeline...")
  (async/go
    (let [emit-sock (get-node-emit-sock port)]
      (with-open [emitter (zmq/socket context emit-sock)]
        (emitter-connect! emitter port)
        (loop [data (async/<! input-chan)]
          (let [runner (get-runner port emitter)]
            (runner data))
          (or (end-of-stream? data)
              (recur (async/<! input-chan))))))))

(defn threaded-pipeline! [context input-chan port threads]
  (log/info "Starting threaded pipeline...")
  (doseq [_ (range 0 threads)]
    (single-pipeline! context input-chan port)))

(defn- signal-end-of-stream [port input-chan]
  (let [threads (get-threads port)]
    (doseq [_ (range 0 threads)]
      (log/info "Passing :end-of-stream to input channel")
      (async/>! input-chan :end-of-stream))))

(defn- received-all-end-of-stream? [end-received port]
  (= (inc end-received) (count (:in (get-node port)))))


(defn run-node! [context consume-sock port]
  (log/info "Running node at port: " port)
  (async/go
    (with-open [consumer (doto (zmq/socket context consume-sock)
                           (zmq/bind (str "tcp://*:" port)))]

      (let [input-chan (async/chan 1)
            threads (get-threads port)]

        (threaded-pipeline! context input-chan port threads)

        (loop [data (utils/read-sock consumer)
               end-received 0]
          (log/info "Read " data "from consumer socket")
          (if (end-of-stream? data)
            ;; TODO: Doesn't work properly.( Race Conditions)
            (if (utils/debug (received-all-end-of-stream? end-received port))
              (doseq [_ (range 0 threads)]
                (log/info "Passing :end-of-stream to input channel")
                (async/>! input-chan :end-of-stream))
              (recur (utils/read-sock consumer) (inc end-received)))

            (do
              (log/info "Pushing data to input channel: (Data " data ")")
              (async/>! input-chan data)
              (recur (utils/read-sock consumer) end-received))))))))

(def node-properties {:map    {:runner       #'nmap
                               :emit-sock    :push
                               :consume-sock :pull}
                      :reduce {:runner       #'nreduce
                               :emit-sock    :push
                               :consume-sock :pull}
                      :shell  {:runner       #'nshell
                               :emit-sock    :push
                               :consume-sock :pull}})

(defn run-grubber! [node context]
  ;; node contains :type :fn and :out
  (let [properties ((:type node) node-properties)
        consume-sock (:consume-sock properties)
        port (get-available-port)]
    (update-node-context port (->NodeCtx node properties nil nil))
    (run-node! @context consume-sock port)
    port))
