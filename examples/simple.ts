import { Remotify, Listen } from "../src/remotify";
import Redis from "ioredis";

class Squarer {
	public async square(x: number) {
		// maybe do some more interesting stuff here
		return x ** 2;
	}
}

const pub = new Redis();
const sub = new Redis();

async function clientProcess() {
	const backend = new Remotify("backend", { pub, sub });
	const remoteSquarer = backend.remotifyClass(Squarer);
	for (const x of [1, 2, 3, 4, 5, 11]) {
		const res = await remoteSquarer.square(x);
		// typeof res is number as expected
		console.log(x, "^2 =", res);
	}
}
async function serverProcess() {
	const squarer = new Squarer();

	const r = new Listen("backend", { pub, sub });
	// add methods to RPC interface with a name like Squarer.square()
	r.listenAll(squarer);
}

if (process.argv[2]) {
	// ts-node examples/simple.ts client
	clientProcess();
} else {
	// ts-node examples/simple.ts
	serverProcess();
}
