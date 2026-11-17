"""Loopback transport fault injection for TestRecoverWorkerStart."""

import asyncio
import sys
from queue import SimpleQueue
from unittest.mock import MagicMock

import grpc
from hatchet_sdk import Context, EmptyModel, Hatchet
from hatchet_sdk.clients.dispatcher.action_listener import ActionListener
from hatchet_sdk.config import ClientConfig, ClientTLSConfig
from hatchet_sdk.worker.runner.runner import Runner


class LostReply(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


async def main(address, worker, scenario, target, barrier):
    config = ClientConfig.model_construct(
        host_port=address,
        server_url="http://" + address,
        tls_config=ClientTLSConfig(strategy="none"),
        token="test",
    )
    client = ActionListener(config, worker)
    connect = client.get_listen_client
    dropped = False
    claims = []
    opens = 0

    async def listen():
        nonlocal opens
        stream = await connect()
        opens += 1
        claim_rpc = client.aio_client.ClaimDelivery
        read = stream.read

        async def receive():
            nonlocal dropped
            message = await read()
            if message is grpc.aio.EOF or message.task_run_external_id != target:
                return message
            if scenario in ("lost_start", "heartbeat_replay") and not dropped:
                dropped = True
                if scenario == "lost_start":
                    stream.cancel()
                    return grpc.aio.EOF
                return await read()
            return message

        async def claim(request, **kwargs):
            nonlocal dropped
            if scenario == "lost_receipt" and not dropped:
                dropped = True
                claims.append((request.receipt_id, None))
                raise LostReply()
            response = await claim_rpc(request, **kwargs)
            claims.append((request.receipt_id, response.accepted))
            if scenario == "lost_reply" and not dropped:
                assert response.accepted
                dropped = True
                raise LostReply()
            return response

        stream.read = receive
        client.aio_client.ClaimDelivery = claim
        return stream

    client.get_listen_client = listen
    executed = []

    @Hatchet(config=config).task(name="execute")
    async def execute(input: EmptyModel, ctx: Context) -> None:
        executed.append(ctx.task_run_id)

    runner = Runner(
        event_queue=SimpleQueue(),
        config=config,
        slots=1,
        durable_slots=0,
        handle_kill=False,
        action_registry={"test:execute": execute._task},
        labels=[],
        lifespan_context=None,
        log_sender=MagicMock(),
    )
    async for action in client:
        assert action is not None
        error = await runner.handle_start_step_run(action)
        assert error is None, error
        assert not any(runner.used_slots_by_pool.values()), runner.used_slots_by_pool
        if action.step_run_id == barrier:
            break
    assert executed == [target, barrier], executed
    assert dropped and any(accepted is False for _, accepted in claims), claims
    if scenario == "lost_start":
        assert opens >= 2, opens
    elif scenario == "lost_receipt":
        assert claims[0][0] == claims[1][0] and claims[1][1], claims
    elif scenario == "lost_reply":
        assert claims[0] == claims[1] and claims[0][1], claims
    elif scenario == "heartbeat_replay":
        assert opens == 1, opens
    client.stop_stream()
    if client.heartbeat_task:
        client.heartbeat_task.cancel()
    print(scenario, "target executed once", "streams", opens, "claims", len(claims))


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(*sys.argv[1:]), timeout=25))
