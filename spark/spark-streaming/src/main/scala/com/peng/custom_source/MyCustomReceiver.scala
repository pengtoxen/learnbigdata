package com.peng.custom_source

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

//socket
class MyCustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) with Logging {

    def onStart() {
        // 启动一个线程，开始接收数据
        new Thread("Socket Receiver") {
            override def run() {
                receive()
            }
        }.start()

        //数据库里面读取数据
    }

    /** Create a socket connection and receive data until receiver is stopped */
    private def receive() {
        var socket: Socket = null
        var userInput: String = null
        try {
            logInfo("Connecting to " + host + ":" + port)
            socket = new Socket(host, port)
            logInfo("Connected to " + host + ":" + port)

            val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))

            userInput = reader.readLine()
            while (!isStopped && userInput != null) {
                //把这个接收到到数据写到下游
                store(userInput)
                userInput = reader.readLine()
            }
            reader.close()
            socket.close()
            logInfo("Stopped receiving")
            restart("Trying to connect again")
        } catch {
            case e: java.net.ConnectException =>
                restart("Error connecting to " + host + ":" + port, e)
            case t: Throwable =>
                restart("Error receiving data", t)
        }
    }

    def onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself isStopped() returns false

        //关闭连接
    }

}
