interface Message { language: string; answer: number }
const message: Message = { language: 'typescript', answer: 6 * 7 };
export default { fetch(): Response { return Response.json(message); } };
