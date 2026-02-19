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

'use client'

import React from 'react'
import { Leva } from 'leva'
import { Common, View } from '@/components/canvas/View'
import dynamic from 'next/dynamic'

const GenmediaExample = dynamic(
  () =>
    import('@/components/canvas/Genmedia').then((mod) => mod.GenmediaExample),
  { ssr: false },
)

export default function Page() {
  return (
    <div className='flex h-screen w-full flex-row'>
      <canvas hidden id='canvas' width='1024' height='1024'></canvas>
      <div className='flex w-full flex-col items-center justify-center space-y-4'>
        <h1 className='text-5xl font-bold leading-tight absolute top-12 left-12'>
          Action Engine <br />x GenMedia
        </h1>

        <View className='h-full w-full'>
          <Common />
          <GenmediaExample />
        </View>
      </div>
      <div className='flex w-[360px] h-full bg-zinc-600'>
        <div className='w-full h-1/3'>
          <Leva oneLineLabels flat fill titleBar={{ drag: false }} />
        </div>
      </div>
    </div>
  )
}
