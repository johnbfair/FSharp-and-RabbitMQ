module RabbitSample

    open System
    open System.Xml
    open System.Xml.Linq
    open FSharp_RabbitMq.Types
       
    type TheGrid(ack, message1CmdQ: RabbitMqPublisher, message2CmdQ: RabbitMqPublisher, count:int) =
        let log msg value = printfn "Message: %s Value: %i" msg value

        let supervisor = new Agent<System.Exception>(fun inbox ->
                            let rec Loop() =
                                async {
                                    let! err = inbox.Receive()
                                    log err.Message 0
                                    do! Loop() }
                            Loop()) |> Agent.start

        let mainQueueAccountant = 
            new Agent<float>(fun inbox ->
                let rec Loop totalSeconds totalCount =
                    async {
                        let! msg = inbox.Receive()
                        let totalSeconds' = totalSeconds + msg
                        let totalCount' = totalCount - 1
                        
                        if (totalCount' = 0) then 
                            printfn "MainQueueAccountant processed %f messages/sec" ((float)count/totalSeconds')

                        do! Loop totalSeconds' totalCount' }
                Loop 0. count) |> Agent.start
       
        member this.RouteCommand(extMsg:string, tag:uint64) =
            (new Agent<Command>(fun inbox ->
                async {
                        let watch = System.Diagnostics.Stopwatch.StartNew()

                        let! msg = inbox.Receive()
                        let msgType =
//                            let xml = XDocument.Parse(msg.Message)
//                            match xml.Root with
//                            | null -> raise (new Exception "Couldn't determine message type (null root)")
//                            | root -> root.Name.LocalName

                            match System.Text.RegularExpressions.Regex.Match(msg.Message, "<(?<tag>\w*)>") with
                            | x when x.Success -> x.Groups.[1].Value
                            | _ -> raise (new Exception "Couldn't determine message type (null root)")

                        match msgType.ToLower() with
                        | "message1" ->                           
                            (new Agent<Command>(fun inbox ->
                                                    async {
                                                        let! x = inbox.Receive()
                                                        let! m = message1CmdQ.Send x.Message
                                                        ack x.Tag
                                                    }) |> Agent.reportErrorsTo supervisor |> Agent.start) 
                            |> fun x-> x.Post msg

                        | "message2" -> 
                            (new Agent<Command>(fun inbox ->
                                                    async {
                                                        let! x = inbox.Receive()
                                                        let! m = message2CmdQ.Send x.Message
                                                        ack x.Tag
                                                    }) |> Agent.reportErrorsTo supervisor |> Agent.start) 
                            |> fun x-> x.Post msg
                        | _ -> ack msg.Tag; raise (new Exception "Invalid Command") 
                        
                        mainQueueAccountant.Post watch.Elapsed.TotalSeconds
                        })
                |> Agent.reportErrorsTo supervisor 
                |> Agent.start)
                |> fun x-> x.Post {Message=extMsg; Tag=tag}

        member this.ProcessMessage1Command ack (extMsg:string, tag:uint64) =
            (new Agent<Message1>(fun inbox ->
                async {
                    let! msg = inbox.Receive()
                    //log msg.Sample1 msg.Value1
                    ack msg.MessageTag
                })
            |> Agent.reportErrorsTo supervisor 
            |> Agent.start)
            |> fun x-> 
                let msg = Rock.Framework.Serialization.XmlObjectSerializer.XmlToObject<Message1>(extMsg) 
                msg.MessageTag <- tag
                msg |> x.Post

        member this.ProcessMessage2Command ack (extMsg:string, tag:uint64) =
            (new Agent<Message2>(fun inbox ->
                async {
                    let! msg = inbox.Receive()
                    //log msg.Sample2 msg.Value2
                    ack msg.MessageTag
                })
            |> Agent.reportErrorsTo supervisor 
            |> Agent.start)
            |> fun x-> 
                let msg = Rock.Framework.Serialization.XmlObjectSerializer.XmlToObject<Message2>(extMsg) 
                msg.MessageTag <- tag
                msg |> x.Post
