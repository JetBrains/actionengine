/**
 * Copyright 2026 The Action Engine Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { JSX, useEffect, useRef, useState } from 'react'
import { marked } from 'marked'
import { v4 as uuidv4 } from 'uuid'

import styles from './Chat.module.css'

export interface ChatMessage {
  id: string
  text: string
  children?: JSX.Element | JSX.Element[]
  sender: string
}

export interface ChatProps {
  name?: string
  messages: ChatMessage[]
  sendMessage: (msg: ChatMessage) => Promise<void>
  disableInput?: boolean
  disabledInputMessage?: string
  className?: string
  placeholder?: string
}

export function Chat(props: ChatProps) {
  const { messages } = props
  const [input, setInput] = useState('')

  const lengthRef = useRef(0)
  const messagesEndRef = useRef(null)
  useEffect(() => {
    if (lengthRef.current === messages.length) return
    lengthRef.current = messages.length
    messagesEndRef.current?.scrollIntoView({
      behavior: 'smooth',
      block: 'start',
    })
  }, [messages])

  const className = props.className || 'h-full max-w-full w-full'

  const numRowsInInput = Math.max(1, input.split('\n').length)
  const [nextMsgId, setNextMsgId] = useState(uuidv4())
  // if one child, make it an array
  const standardizedChildren = (msg: ChatMessage) => {
    if (!msg.children) return undefined
    if (Array.isArray(msg.children)) return msg.children
    return [msg.children]
  }
  return (
    <>
      <div className={className}>
        <div className='flex max-h-full h-full w-full max-w-full flex-col space-y-4 rounded-lg bg-white p-4 shadow-lg'>
          <h1 className='max-h-fit text-center text-2xl font-bold'>
            {props.name || 'Echo action'}
          </h1>
          <div className='max-h-full h-full overflow-y-auto rounded-md border bg-gray-50 p-4'>
            {messages.map((msg, idx) => {
              const markedText = { __html: marked(msg.text || '') }
              // @ts-ignore
              // @ts-ignore
              // @ts-ignore
              return (
                <div
                  key={msg.id}
                  ref={idx === messages.length - 1 ? messagesEndRef : null}
                  className={`mb-2 flex wrap ${
                    msg.sender === 'You' ? 'justify-end' : 'justify-start'
                  }`}
                >
                  <div
                    className={`rounded px-4 py-2 text-white max-w-full ${
                      msg.sender === 'You' ? 'bg-green-500' : 'bg-blue-500'
                    }`}
                  >
                    <span className='block text-xs'>{msg.sender}</span>
                    {msg.children ? (
                      standardizedChildren(msg)?.map((child, cidx) => (
                        <div key={`${msg.id}-${cidx}`} className='mt-2'>
                          {child}
                        </div>
                      ))
                    ) : (
                      <div
                        dangerouslySetInnerHTML={markedText}
                        className={`max-w-full ${styles.message}`}
                      />
                    )}
                  </div>
                </div>
              )
            })}
          </div>

          <form
            onSubmit={async (e) => {
              e.preventDefault()
              if (!input.trim()) return
              await props.sendMessage({
                id: nextMsgId,
                text: input,
                sender: 'You',
              })
              setNextMsgId(uuidv4())
              setInput('')
            }}
            className='mt-4 flex items-end space-x-2'
          >
            <textarea
              className={`flex-1 rounded border px-3 py-2 ${props.disableInput ? 'bg-gray-50' : 'bg-white'}`}
              rows={Math.min(10, numRowsInInput)}
              placeholder={
                props.disableInput
                  ? props.disabledInputMessage || 'Establishing connection...'
                  : props.placeholder || 'Type a message'
              }
              value={input}
              disabled={props.disableInput}
              onKeyDown={async (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  if (!input.trim()) return

                  await props.sendMessage({
                    id: nextMsgId,
                    text: input,
                    sender: 'You',
                  })
                  setNextMsgId(uuidv4())
                  setInput('')
                }
              }}
              onChange={(e) => setInput(e.target.value)}
            />
            <button
              type='submit'
              className='max-h-12 rounded bg-blue-600 px-4 py-2 text-white hover:bg-blue-700'
            >
              Send
            </button>
          </form>
        </div>
      </div>
    </>
  )
}
