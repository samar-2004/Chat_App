
import amqp from "amqplib"
import dotenv from "dotenv"

dotenv.config()

let channel: amqp.Channel

export const connectRabbitMQ = async () => {

    try {
        const connection = await amqp.connect({
            protocol:"amqp",
            hostname: process.env.Rabbitmq_Host,
            port: 5672,
            username: process.env.Rabbitmq_Username,
            password: process.env.Rabbitmq_Password,
        })
        channel = await connection.createChannel()
        console.log("✅Connected to RabbitMQ")

        connection.on("error", (err) => {
            console.error("RabbitMQ connection error:", err)
        })

        connection.on("close", () => {
            console.log("RabbitMQ connection closed")
        })
    } catch (error) {
        console.error("Failed to connect to RabbitMQ:", error)
        process.exit(1)
    }
}

export const getChannel = (): amqp.Channel => {
    if (!channel) {
        throw new Error("RabbitMQ channel not initialized. Call connectRabbitMQ first.")
    }
    return channel
}

export const publishToQueue = async (queueName: string, message: any) => {
    const ch = getChannel()
    await ch.assertQueue(queueName, { durable: true })
    ch.sendToQueue(queueName, Buffer.from(JSON.stringify(message)), { persistent: true })
}

export const consumeMessage = async (queue: string, callback: (msg: object) => void) => {
    const ch = getChannel()
    await ch.assertQueue(queue, { durable: true })
    ch.consume(queue, (msg) => {
        if (msg) {
            const content = JSON.parse(msg.content.toString())
            callback(content)
            ch.ack(msg)
        }
    })
}

export const closeRabbitMQ = async () => {
    try {
        if (channel) await channel.close()
        if (connection) await connection.close()
        console.log("RabbitMQ connection closed gracefully")
    } catch (error) {
        console.error("Error closing RabbitMQ connection:", error)
    }
}