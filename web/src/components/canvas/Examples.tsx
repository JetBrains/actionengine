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

import { MeshDistortMaterial } from '@react-three/drei'
import { useRef, useState } from 'react'
import { useCursor } from '@react-three/drei'
import * as THREE from 'three'
import { useFrame } from '@react-three/fiber'

export const Blob = ({ ...props }) => {
  const [hovered, hover] = useState(false)
  useCursor(hovered)

  const meshRef = useRef<THREE.Mesh>(null)
  useFrame((_, delta) => {
    if (meshRef.current) {
      meshRef.current.position.set(
        props.position?.[0] || 0,
        props.position?.[1] || 0,
        props.position?.[2] || 0,
      )
      // meshRef.current.rotation.x += delta * 0.1
    }
  })

  return (
    <mesh
      onClick={() => props.onClick?.()}
      onPointerOver={() => hover(true)}
      onPointerOut={() => hover(false)}
      ref={meshRef}
      {...props}
    >
      <sphereGeometry args={[1, 64, 64]} />
      <MeshDistortMaterial
        roughness={0.5}
        color={hovered ? 'hotpink' : props.color}
        distort={0.4}
        speed={2}
      />
    </mesh>
  )
}
