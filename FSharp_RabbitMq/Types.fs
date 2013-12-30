namespace FSharp_RabbitMq.Types

open RabbitMQ.Client

type Agent<'T> = MailboxProcessor<'T>

type Credentials = { Host:string; Username:string; Password:string }

type Command = {Message:string; Tag:uint64}

type PublishType =
    | Exchange of string
    | Queue of string

type CounterMsg =
    | Add of int64
    | GetAndReset of (int64 -> unit)

type Message1(sample1, value1, messageTag) = 
    member val Sample1 = sample1 with get,set
    member val Value1 = value1 with get,set
    member val MessageTag = messageTag with get,set
    new () = Message1("", 0, 0UL)

type Message2(sample2, value2, messageTag) = 
    member val Sample2 = sample2 with get,set
    member val Value2 = value2 with get,set
    member val MessageTag = messageTag with get,set
    new () = Message2("", 0, 0UL)
        
type RabbitMqPublisher (creds, publishType) = 
    let connection = 
        let connectionFactory = new ConnectionFactory()
        connectionFactory.UserName <- creds.Username
        connectionFactory.Password <- creds.Password
        connectionFactory.Uri <- creds.Host
        connectionFactory.CreateConnection()
    let model = 
        let m = connection.CreateModel()
        match publishType with
        | Queue x -> m.QueueDeclare(x, true, false, false, null) |> ignore
        | _ -> ()
        m
    let properties = 
        let p = model.CreateBasicProperties()
        p.DeliveryMode <- 2uy; p
    member this.Send (msg:string) = 
        match publishType with
        | Queue x -> async { model.BasicPublish("", x, properties, System.Text.Encoding.ASCII.GetBytes msg) }
        | Exchange x -> async { model.BasicPublish(x, "", properties, System.Text.Encoding.ASCII.GetBytes msg) }

type RabbitMqSubscriber(creds:Credentials, queue: string) = 
    let connection = 
        let connectionFactory = new ConnectionFactory()
        connectionFactory.Uri <- creds.Host
        connectionFactory.UserName <- creds.Username
        connectionFactory.Password <- creds.Password
        connectionFactory.CreateConnection()
    let model = 
        let m = connection.CreateModel()
        m.QueueDeclare(queue, true, false, false, null) |> ignore
        //m.QueueBind(queue, "amq.direct", "")
        m
    let receiveMessage f = new Events.BasicDeliverEventHandler(fun sender args -> f (System.Text.Encoding.ASCII.GetString args.Body) args.DeliveryTag)
    let consumer = new Events.EventingBasicConsumer (Model= model)
    member this.BindReceivedEvent f = consumer.add_Received(receiveMessage f)
    member this.Start = model.BasicConsume(queue, false, consumer)
    member this.Working = consumer.IsRunning
    member this.AckMessage tag = model.BasicAck(tag, false)

module Agent =
    let reportErrorsTo (supervisor: Agent<exn>) (agent: Agent<_>) =
        agent.Error.Add(fun error -> supervisor.Post error); agent

    let start (agent: Agent<_>) = agent.Start(); agent   