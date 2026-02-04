'use client';

import { useChat } from '@ai-sdk/react';
import { DefaultChatTransport } from 'ai';
import { useState, useEffect, useRef } from 'react';

export default function Chat() {
  const [input, setInput] = useState('');
  // Initialize conversationId immediately from localStorage or generate new one
  const [conversationId, setConversationId] = useState(() => {
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem('fleetlm-conversation-id');
      if (saved) return saved;
      const newId = `chat-${Date.now()}`;
      localStorage.setItem('fleetlm-conversation-id', newId);
      return newId;
    }
    return `chat-${Date.now()}`;
  });
  const [isLoadingHistory, setIsLoadingHistory] = useState(true);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const { messages, setMessages, sendMessage, status } = useChat({
    transport: new DefaultChatTransport({
      body: { conversationId },
    }),
  });

  const isLoading = status === 'submitted' || status === 'streaming';

  // Auto-scroll to bottom when messages change
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, status]);

  // Fetch history on mount
  useEffect(() => {
    // Fetch message history
    fetch('/api/chat/history', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ conversationId }),
    })
      .then(res => res.json())
      .then(data => {
        if (data.messages && data.messages.length > 0) {
          setMessages(data.messages);
        }
      })
      .catch(err => console.error('Failed to load history:', err))
      .finally(() => setIsLoadingHistory(false));
  }, [conversationId, setMessages]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    // Send message with conversationId in body
    sendMessage({ text: input }, { body: { conversationId } });
    setInput('');
  };

  const handleConversationChange = (newConversationId: string) => {
    setConversationId(newConversationId);
    localStorage.setItem('fleetlm-conversation-id', newConversationId);
    setMessages([]);

    // Fetch history for new conversation
    setIsLoadingHistory(true);
    fetch('/api/chat/history', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ conversationId: newConversationId }),
    })
      .then(res => res.json())
      .then(data => {
        if (data.messages && data.messages.length > 0) {
          setMessages(data.messages);
        }
      })
      .catch(err => console.error('Failed to load history:', err))
      .finally(() => setIsLoadingHistory(false));
  };

  const handleNewChat = () => {
    const newConversationId = `chat-${Date.now()}`;
    handleConversationChange(newConversationId);
  };

  return (
    <div className="flex h-screen flex-col text-slate-100">
      <header className="sticky top-0 z-10 border-b border-white/10 bg-slate-900">
        <div className="mx-auto max-w-4xl px-4 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-xl font-semibold tracking-tight">FleetLM Chat Example</h1>
              <p className="text-sm text-slate-400">Using gpt-4o-mini</p>
            </div>
            <button
              onClick={handleNewChat}
              className="rounded-lg bg-white/10 px-4 py-2 text-sm font-medium transition hover:bg-white/20"
            >
              New Chat
            </button>
          </div>
          <div className="mt-3 flex items-center gap-2">
            <label className="text-sm text-slate-400">Conversation ID:</label>
            <input
              type="text"
              value={conversationId}
              onChange={(e) => handleConversationChange(e.target.value)}
              className="flex-1 rounded-lg border border-white/10 bg-white/5 px-3 py-1.5 text-sm outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500/30"
              placeholder="Enter conversation ID..."
            />
          </div>
        </div>
      </header>

      <main className="mx-auto flex-1 w-full max-w-4xl overflow-y-auto px-4 py-6">
        {isLoadingHistory ? (
          <div className="text-center text-slate-400">
            <p className="mb-2 text-base font-medium">Loading conversation...</p>
          </div>
        ) : messages.length === 0 ? (
          <div className="text-center text-slate-400">
            <p className="mb-2 text-base font-medium">Start a conversation</p>
            <p className="text-sm">FleetLM stores the message log</p>
          </div>
        ) : null}

        <div className="flex flex-col gap-3">
          {messages.map((message) => (
            <div
              key={message.id}
              className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
            >
              <div
                className={`max-w-[80%] rounded-xl px-4 py-2 shadow-md ${
                  message.role === 'user'
                    ? 'bg-blue-600 text-white'
                    : 'bg-white/5 ring-1 ring-white/10 backdrop-blur'
                }`}
              >
                {message.parts.map((part, i) => {
                  if (part.type === 'text') return <div key={i} className="whitespace-pre-wrap">{part.text}</div>;
                  return null;
                })}
              </div>
            </div>
          ))}

          {isLoading && (
            <div className="flex justify-start">
              <div className="max-w-[80%] rounded-xl bg-white/5 px-4 py-2 ring-1 ring-white/10">
                <div className="flex items-center gap-2">
                  <div className="h-2 w-2 animate-bounce rounded-full bg-slate-400" />
                  <div className="h-2 w-2 animate-bounce rounded-full bg-slate-400 [animation-delay:0.2s]" />
                  <div className="h-2 w-2 animate-bounce rounded-full bg-slate-400 [animation-delay:0.4s]" />
                </div>
              </div>
            </div>
          )}
          {/* Scroll anchor */}
          <div ref={messagesEndRef} />
        </div>
      </main>

      <footer className="border-t border-white/10 bg-slate-900">
        <div className="mx-auto max-w-4xl px-4 py-4">
          <form onSubmit={handleSubmit} className="flex gap-2">
            <input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Type your message..."
              className="flex-1 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-slate-100 placeholder:text-slate-400 outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/30"
              disabled={isLoading}
            />
            <button
              type="submit"
              disabled={isLoading || !input.trim()}
              className="rounded-xl bg-blue-600 px-5 py-2 text-white shadow transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              Send
            </button>
          </form>
        </div>
      </footer>
    </div>
  );
}
